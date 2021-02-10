/*
 * Copyright 2014 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.protobuf.lite;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.grpc.ExperimentalApi;
import io.grpc.HasByteBuffer;
import io.grpc.KnownLength;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.PrototypeMarshaller;
import io.grpc.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.NoSuchMethodException;
import java.util.Iterator;

import java.util.ArrayDeque;
import java.util.Hashtable;

//import io.netty.buffer.ByteBuf;

/**
 * Utility methods for using protobuf with grpc.
 */
@ExperimentalApi("Experimental until Lite is stable in protobuf")
public final class ProtoLiteUtils {

  // default visibility to avoid synthetic accessors
  static volatile ExtensionRegistryLite globalRegistry =
      ExtensionRegistryLite.getEmptyRegistry();

  private static final int BUF_SIZE = 8192;

  /**
   * Assume Java 9+ if it isn't Java 7 or Java 8.
   */
  @VisibleForTesting
  static final boolean IS_JAVA9_OR_HIGHER;

  /**
   * The same value as {@link io.grpc.internal.GrpcUtil#DEFAULT_MAX_MESSAGE_SIZE}.
   */
  @VisibleForTesting
  static final int DEFAULT_MAX_MESSAGE_SIZE = 4 * 1024 * 1024;

  /**
   * Threshold for passing {@link ByteBuffer}s directly into Protobuf.
   */
  @VisibleForTesting
  static final int MESSAGE_ZERO_COPY_THRESHOLD = 64 * 1024;

  static {
    boolean isJava9OrHigher = true;
    try {
      Class.forName("java.lang.StackWalker");
    } catch (ClassNotFoundException e) {
      isJava9OrHigher = false;
    }
    IS_JAVA9_OR_HIGHER = isJava9OrHigher;
  }

  /**
   * Sets the global registry for proto marshalling shared across all servers and clients.
   *
   * <p>Warning:  This API will likely change over time.  It is not possible to have separate
   * registries per Process, Server, Channel, Service, or Method.  This is intentional until there
   * is a more appropriate API to set them.
   *
   * <p>Warning:  Do NOT modify the extension registry after setting it.  It is thread safe to call
   * {@link #setExtensionRegistry}, but not to modify the underlying object.
   *
   * <p>If you need custom parsing behavior for protos, you will need to make your own
   * {@code MethodDescriptor.Marshaller} for the time being.
   *
   * @since 1.0.0
   */
  @ExperimentalApi("https://github.com/grpc/grpc-java/issues/1787")
  public static void setExtensionRegistry(ExtensionRegistryLite newRegistry) {
    globalRegistry = checkNotNull(newRegistry, "newRegistry");
  }

  /**
   * Creates a {@link Marshaller} for protos of the same type as {@code defaultInstance}.
   *
   * @since 1.0.0
   */
  public static <T extends MessageLite> Marshaller<T> marshaller(T defaultInstance) {
    // TODO(ejona): consider changing return type to PrototypeMarshaller (assuming ABI safe)
    return new MessageMarshaller<>(defaultInstance);
  }

  /**
   * Produce a metadata marshaller for a protobuf type.
   *
   * @since 1.0.0
   */
  public static <T extends MessageLite> Metadata.BinaryMarshaller<T> metadataMarshaller(
      T defaultInstance) {
    return new MetadataMarshaller<>(defaultInstance);
  }

  /** Copies the data from input stream to output stream. */
  static long copy(InputStream from, OutputStream to) throws IOException {
    // Copied from guava com.google.common.io.ByteStreams because its API is unstable (beta)
    checkNotNull(from, "inputStream cannot be null!");
    checkNotNull(to, "outputStream cannot be null!");
    byte[] buf = new byte[BUF_SIZE];
    long total = 0;
    while (true) {
      int r = from.read(buf);
      if (r == -1) {
        break;
      }
      to.write(buf, 0, r);
      total += r;
    }
    return total;
  }

  private ProtoLiteUtils() {
  }

  private static final class MessageMarshaller<T extends MessageLite>
      implements PrototypeMarshaller<T> {
    private static final ThreadLocal<Reference<byte[]>> bufs = new ThreadLocal<>();
    private static Method newCisInstance;

    private final Parser<T> parser;
    private final T defaultInstance;

    private ArrayList<List<Object>> byteBufsQueue = new ArrayList<List<Object>>();

    @SuppressWarnings("unchecked")
    MessageMarshaller(T defaultInstance) {
      this.defaultInstance = defaultInstance;
      parser = (Parser<T>) defaultInstance.getParserForType();

      if (newCisInstance == null) {
          try {
                Class[] params = {
                  Iterable.class,
                  boolean.class
                };
                Method method = CodedInputStream.class.getDeclaredMethod("newInstance", params);
                method.setAccessible(true);
                newCisInstance = method;
              } catch (NoSuchMethodException e) {
                throw new RuntimeException(e.toString());
              }
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<T> getMessageClass() {
      // Precisely T since protobuf doesn't let messages extend other messages.
      return (Class<T>) defaultInstance.getClass();
    }

    @Override
    public T getMessagePrototype() {
      return defaultInstance;
    }

    @Override
    public InputStream stream(T value) {
      return new ProtoInputStream(value, parser);
    }

    private Hashtable<Class, Method> retainMethodMap = new Hashtable<Class, Method>();
    private Hashtable<Class, Method> releaseMethodMap = new Hashtable<Class, Method>();

    private void retain(Object obj) {
      try {
        Class cls = obj.getClass();
        Method method = retainMethodMap.get(cls);
        if (method == null) {
          method = cls.getMethod("retain", new Class[0]);
          method.setAccessible(true);
          retainMethodMap.put(cls, method);
        }
        method.invoke(obj);
      } catch (Exception e) {
        System.out.println(e.toString());
        e.printStackTrace();
      }
    }

    private void release(Object obj) {
      try {
        Class cls = obj.getClass();
        Method method = retainMethodMap.get(cls);
        if (method == null) {
          method = cls.getMethod("release", new Class[0]);
          method.setAccessible(true);
          retainMethodMap.put(cls, method);
        }
        method.invoke(obj);
      } catch (Exception e) {
        System.out.println(e.toString());
        e.printStackTrace();
      }
    }

    private void retainByteBufs(ArrayList<Object> byteBufs) {
      for (Object val : byteBufs) {
        retain(val);
      }
      byteBufsQueue.add(byteBufs);

      if (byteBufsQueue.size() > 3) {
        List<Object> removingByteBufs = byteBufsQueue.remove(0);
        for (Object val : removingByteBufs) {
          release(val);
        }
      }
    }

    @Override
    public T parse(InputStream stream) {
      if (stream instanceof ProtoInputStream) {
        ProtoInputStream protoStream = (ProtoInputStream) stream;
        // Optimization for in-memory transport. Returning provided object is safe since protobufs
        // are immutable.
        //
        // However, we can't assume the types match, so we have to verify the parser matches.
        // Today the parser is always the same for a given proto, but that isn't guaranteed. Even
        // if not, using the same MethodDescriptor would ensure the parser matches and permit us
        // to enable this optimization.
        if (protoStream.parser() == parser) {
          try {
            @SuppressWarnings("unchecked")
            T message = (T) ((ProtoInputStream) stream).message();
            return message;
          } catch (IllegalStateException ignored) {
            // Stream must have been read from, which is a strange state. Since the point of this
            // optimization is to be transparent, instead of throwing an error we'll continue,
            // even though it seems likely there's a bug.
          }
        }
      }
      CodedInputStream cis = null;
      try {
        if (stream instanceof KnownLength) {
          int size = stream.available();
          if (size == 0) {
            return defaultInstance;
          }
          if (size >= MESSAGE_ZERO_COPY_THRESHOLD
              && stream instanceof HasByteBuffer
              && ((HasByteBuffer) stream).getByteBufferSupported()
              && stream.markSupported()) {
            ArrayList<Object> byteBufs = null;
            try {
              Field bufferField = stream.getClass().getDeclaredField("buffer");
              bufferField.setAccessible(true);
              Object readableBuffer = bufferField.get(stream);

              Field readableBuffersField = readableBuffer.getClass().getDeclaredField("readableBuffers");
              readableBuffersField.setAccessible(true);
              ArrayDeque<Object> bufferList = (ArrayDeque<Object>)readableBuffersField.get(readableBuffer);
              
              Object[] bufferArray = bufferList.toArray();
              if (bufferArray.length > 0) {
                Field bufferField2 = bufferArray[0].getClass().getDeclaredField("buffer");
                bufferField2.setAccessible(true);
                byteBufs = new ArrayList<Object>();
                for (Object val : bufferArray) {
                  Object byteBuf = bufferField2.get(val);
                  byteBufs.add(byteBuf);
                }
              }
            } catch (Exception e) {
              System.out.println(e.toString());
            }

            if (byteBufs != null && byteBufs.size() > 0) {
              retainByteBufs(byteBufs);
            }

            List<ByteBuffer> buffers = new ArrayList<>();
            stream.mark(size);
            while (stream.available() != 0) {
              ByteBuffer buffer = ((HasByteBuffer) stream).getByteBuffer();
              stream.skip(buffer.remaining());
              buffers.add(buffer);
            }
            
            // ReadableBuffers$BufferInputStream
            // -> final ReadableBuffer buffer; (CompositeReadableBuffer)
            //   -> readableBuffers: ArrayDeque
            //     -> NettyReadableBuffer <- AbstractReadableBuffer <- ReadableBuffer
            //
            // stream.buffer.readableBuffers.toArray()
            // readableBuffer.buffer.retain()

            try {
              cis = (CodedInputStream)newCisInstance.invoke(null, new Object[] { buffers, true });
              cis.enableAliasing(true);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e.toString());
            }
          } else if (size > 0 && size <= DEFAULT_MAX_MESSAGE_SIZE) {
            Reference<byte[]> ref;
            // buf should not be used after this method has returned.
            byte[] buf;
            if ((ref = bufs.get()) == null || (buf = ref.get()) == null || buf.length < size) {
              buf = new byte[size];
              bufs.set(new WeakReference<>(buf));
            }

            int remaining = size;
            while (remaining > 0) {
              int position = size - remaining;
              int count = stream.read(buf, position, remaining);
              if (count == -1) {
                break;
              }
              remaining -= count;
            }

            if (remaining != 0) {
              int position = size - remaining;
              throw new RuntimeException("size inaccurate: " + size + " != " + position);
            }
            cis = CodedInputStream.newInstance(buf, 0, size);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (cis == null) {
        cis = CodedInputStream.newInstance(stream);
      }
      // Pre-create the CodedInputStream so that we can remove the size limit restriction
      // when parsing.
      cis.setSizeLimit(Integer.MAX_VALUE);

      try {
        return parseFrom(cis);
      } catch (InvalidProtocolBufferException ipbe) {
        throw Status.INTERNAL.withDescription("Invalid protobuf byte sequence")
            .withCause(ipbe).asRuntimeException();
      }
    }

    private T parseFrom(CodedInputStream stream) throws InvalidProtocolBufferException {
      T message = parser.parseFrom(stream, globalRegistry);
      try {
        stream.checkLastTagWas(0);
        return message;
      } catch (InvalidProtocolBufferException e) {
        e.setUnfinishedMessage(message);
        throw e;
      }
    }
  }

  private static final class MetadataMarshaller<T extends MessageLite>
      implements Metadata.BinaryMarshaller<T> {

    private final T defaultInstance;

    MetadataMarshaller(T defaultInstance) {
      this.defaultInstance = defaultInstance;
    }

    @Override
    public byte[] toBytes(T value) {
      return value.toByteArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T parseBytes(byte[] serialized) {
      try {
        return (T) defaultInstance.getParserForType().parseFrom(serialized, globalRegistry);
      } catch (InvalidProtocolBufferException ipbe) {
        throw new IllegalArgumentException(ipbe);
      }
    }
  }
}
