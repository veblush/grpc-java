/*
 * Copyright 2018 The gRPC Authors
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

package io.grpc.internal;

import static com.google.common.truth.Truth.assertThat;
import static io.grpc.internal.ServiceConfigInterceptor.HEDGING_POLICY_KEY;
import static io.grpc.internal.ServiceConfigInterceptor.RETRY_POLICY_KEY;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Deadline;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.internal.ManagedChannelServiceConfig.MethodInfo;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link ServiceConfigInterceptor}.
 */
@RunWith(JUnit4.class)
public class ServiceConfigInterceptorTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Mock private Channel channel;
  @Captor private ArgumentCaptor<CallOptions> callOptionsCap;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  private final ServiceConfigInterceptor interceptor =
      new ServiceConfigInterceptor(/* retryEnabled = */ true);

  private final String fullMethodName =
      MethodDescriptor.generateFullMethodName("service", "method");
  private final MethodDescriptor<Void, Void> methodDescriptor =
      MethodDescriptor.newBuilder(new NoopMarshaller(), new NoopMarshaller())
          .setType(MethodType.UNARY)
          .setFullMethodName(fullMethodName)
          .build();

  private static final class JsonObj extends HashMap<String, Object> {
    private JsonObj(Object ... kv) {
      for (int i = 0; i < kv.length; i += 2) {
        put((String) kv[i], kv[i + 1]);
      }
    }
  }

  private static final class JsonList extends ArrayList<Object> {
    private JsonList(Object ... values) {
      addAll(Arrays.asList(values));
    }
  }

  @Test
  public void withWaitForReady() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "waitForReady", true);
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    interceptor.interceptCall(methodDescriptor, CallOptions.DEFAULT.withoutWaitForReady(), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().isWaitForReady()).isTrue();
  }

  @Test
  public void handleNullConfig() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "waitForReady", true);
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));

    interceptor.handleUpdate(createManagedChannelServiceConfig(serviceConfig));
    interceptor.handleUpdate(null);

    interceptor.interceptCall(methodDescriptor, CallOptions.DEFAULT.withoutWaitForReady(), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().isWaitForReady()).isFalse();
  }

  @Test
  public void handleUpdateNotCalledBeforeInterceptCall() {
    interceptor.interceptCall(methodDescriptor, CallOptions.DEFAULT.withoutWaitForReady(), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().isWaitForReady()).isFalse();
    assertThat(callOptionsCap.getValue().getOption(RETRY_POLICY_KEY).get())
        .isNull();
    assertThat(callOptionsCap.getValue().getOption(HEDGING_POLICY_KEY).get())
        .isNull();
  }

  @Test
  public void withMaxRequestSize() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "maxRequestMessageBytes", 1d);
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    interceptor.interceptCall(methodDescriptor, CallOptions.DEFAULT, channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getMaxOutboundMessageSize()).isEqualTo(1);
  }

  @Test
  public void withMaxRequestSize_pickSmallerExisting() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "maxRequestMessageBytes", 10d);
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    interceptor.interceptCall(
        methodDescriptor, CallOptions.DEFAULT.withMaxOutboundMessageSize(5), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getMaxOutboundMessageSize()).isEqualTo(5);
  }

  @Test
  public void withMaxRequestSize_pickSmallerNew() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "maxRequestMessageBytes", 5d);
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    interceptor.interceptCall(
        methodDescriptor, CallOptions.DEFAULT.withMaxOutboundMessageSize(10), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getMaxOutboundMessageSize()).isEqualTo(5);
  }

  @Test
  public void withMaxResponseSize() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "maxResponseMessageBytes", 1d);
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    interceptor.interceptCall(methodDescriptor, CallOptions.DEFAULT, channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getMaxInboundMessageSize()).isEqualTo(1);
  }

  @Test
  public void withMaxResponseSize_pickSmallerExisting() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "maxResponseMessageBytes", 5d);
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    interceptor.interceptCall(
        methodDescriptor, CallOptions.DEFAULT.withMaxInboundMessageSize(10), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getMaxInboundMessageSize()).isEqualTo(5);
  }

  @Test
  public void withMaxResponseSize_pickSmallerNew() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "maxResponseMessageBytes", 10d);
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    interceptor.interceptCall(
        methodDescriptor, CallOptions.DEFAULT.withMaxInboundMessageSize(5), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getMaxInboundMessageSize()).isEqualTo(5);
  }

  @Test
  public void withoutWaitForReady() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "waitForReady", false);
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    interceptor.interceptCall(methodDescriptor, CallOptions.DEFAULT.withWaitForReady(), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().isWaitForReady()).isFalse();
  }

  @Test
  public void fullMethodMatched() {
    // Put in service that matches, but has no deadline.  It should be lower priority
    JsonObj name1 = new JsonObj("service", "service");
    JsonObj methodConfig1 = new JsonObj("name", new JsonList(name1));

    JsonObj name2 = new JsonObj("service", "service", "method", "method");
    JsonObj methodConfig2 = new JsonObj("name", new JsonList(name2), "timeout", "1s");

    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig1, methodConfig2));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    interceptor.interceptCall(methodDescriptor, CallOptions.DEFAULT, channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getDeadline()).isNotNull();
  }

  @Test
  public void nearerDeadlineKept_existing() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "timeout", "100000s");
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    Deadline existingDeadline = Deadline.after(1000, TimeUnit.NANOSECONDS);
    interceptor.interceptCall(
        methodDescriptor, CallOptions.DEFAULT.withDeadline(existingDeadline), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getDeadline()).isEqualTo(existingDeadline);
  }

  @Test
  public void nearerDeadlineKept_new() {
    // TODO(carl-mastrangelo): the deadlines are very large because they change over time.
    // This should be fixed, and is tracked in https://github.com/grpc/grpc-java/issues/2531
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "timeout", "1s");
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    Deadline existingDeadline = Deadline.after(1234567890, TimeUnit.NANOSECONDS);
    interceptor.interceptCall(
        methodDescriptor, CallOptions.DEFAULT.withDeadline(existingDeadline), channel);

    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getDeadline()).isNotEqualTo(existingDeadline);
  }

  @Test
  public void handleUpdate_onEmptyName() {
    JsonObj methodConfig = new JsonObj();
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    assertThat(interceptor.managedChannelServiceConfig.get().getDefaultMethodConfig()).isNull();
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMap()).isEmpty();
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMethodMap()).isEmpty();
  }

  @Test
  public void handleUpdate_onDefaultMethodConfig() {
    JsonObj name = new JsonObj();
    JsonObj methodConfig = new JsonObj("name", new JsonList(name));
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);
    interceptor.handleUpdate(parsedServiceConfig);
    assertThat(interceptor.managedChannelServiceConfig.get().getDefaultMethodConfig())
        .isEqualTo(new MethodInfo(methodConfig, false, 1, 1));
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMap()).isEmpty();
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMethodMap()).isEmpty();

    name = new JsonObj("method", "");
    methodConfig = new JsonObj("name", new JsonList(name));
    serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    parsedServiceConfig = createManagedChannelServiceConfig(serviceConfig);
    interceptor.handleUpdate(parsedServiceConfig);
    assertThat(interceptor.managedChannelServiceConfig.get().getDefaultMethodConfig())
        .isEqualTo(new MethodInfo(methodConfig, false, 1, 1));
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMap()).isEmpty();
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMethodMap()).isEmpty();

    name = new JsonObj("service", "");
    methodConfig = new JsonObj("name", new JsonList(name));
    serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    parsedServiceConfig = createManagedChannelServiceConfig(serviceConfig);
    interceptor.handleUpdate(parsedServiceConfig);
    assertThat(interceptor.managedChannelServiceConfig.get().getDefaultMethodConfig())
        .isEqualTo(new MethodInfo(methodConfig, false, 1, 1));
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMap()).isEmpty();
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMethodMap()).isEmpty();

    name = new JsonObj("service", "", "method", "");
    methodConfig = new JsonObj("name", new JsonList(name));
    serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    parsedServiceConfig = createManagedChannelServiceConfig(serviceConfig);
    interceptor.handleUpdate(parsedServiceConfig);
    assertThat(interceptor.managedChannelServiceConfig.get().getDefaultMethodConfig())
        .isEqualTo(new MethodInfo(methodConfig, false, 1, 1));
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMap()).isEmpty();
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMethodMap()).isEmpty();
  }

  @Test
  public void handleUpdate_replaceExistingConfig() {
    JsonObj name1 = new JsonObj("service", "service");
    JsonObj methodConfig1 = new JsonObj("name", new JsonList(name1));
    JsonObj serviceConfig1 = new JsonObj("methodConfig", new JsonList(methodConfig1));

    JsonObj name2 = new JsonObj("service", "service", "method", "method");
    JsonObj methodConfig2 = new JsonObj("name", new JsonList(name2));
    JsonObj serviceConfig2 = new JsonObj("methodConfig", new JsonList(methodConfig2));
    ManagedChannelServiceConfig parsedServiceConfig1 =
        createManagedChannelServiceConfig(serviceConfig1);
    ManagedChannelServiceConfig parsedServiceConfig2 =
        createManagedChannelServiceConfig(serviceConfig2);

    interceptor.handleUpdate(parsedServiceConfig1);

    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMap()).isNotEmpty();
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMethodMap()).isEmpty();

    interceptor.handleUpdate(parsedServiceConfig2);

    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMap()).isEmpty();
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMethodMap()).isNotEmpty();
  }

  @Test
  public void handleUpdate_matchNames() {
    JsonObj name1 = new JsonObj("service", "service2");
    JsonObj name2 = new JsonObj("service", "service", "method", "method");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name1, name2));
    JsonObj serviceConfig = new JsonObj("methodConfig", new JsonList(methodConfig));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMethodMap())
        .containsExactly(
            methodDescriptor.getFullMethodName(),
            new MethodInfo(methodConfig, false, 1, 1));
    assertThat(interceptor.managedChannelServiceConfig.get().getServiceMap()).containsExactly(
        "service2", new MethodInfo(methodConfig, false, 1, 1));
  }

  @Test
  public void interceptCall_matchNames() {
    JsonObj name0 = new JsonObj();
    JsonObj name1 = new JsonObj("service", "service");
    JsonObj name2 = new JsonObj("service", "service", "method", "method");
    JsonObj methodConfig0 = new JsonObj(
        "name", new JsonList(name0), "maxRequestMessageBytes", 5d);
    JsonObj methodConfig1 = new JsonObj(
        "name", new JsonList(name1), "maxRequestMessageBytes", 6d);
    JsonObj methodConfig2 = new JsonObj(
        "name", new JsonList(name2), "maxRequestMessageBytes", 7d);
    JsonObj serviceConfig =
        new JsonObj("methodConfig", new JsonList(methodConfig0, methodConfig1, methodConfig2));
    ManagedChannelServiceConfig parsedServiceConfig =
        createManagedChannelServiceConfig(serviceConfig);

    interceptor.handleUpdate(parsedServiceConfig);

    String fullMethodName =
        MethodDescriptor.generateFullMethodName("service", "method");
    MethodDescriptor<Void, Void> methodDescriptor =
        MethodDescriptor.newBuilder(new NoopMarshaller(), new NoopMarshaller())
            .setType(MethodType.UNARY)
            .setFullMethodName(fullMethodName)
            .build();
    interceptor.interceptCall(
        methodDescriptor, CallOptions.DEFAULT, channel);
    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getMaxOutboundMessageSize()).isEqualTo(7);

    fullMethodName =
        MethodDescriptor.generateFullMethodName("service", "method2");
    methodDescriptor =
        MethodDescriptor.newBuilder(new NoopMarshaller(), new NoopMarshaller())
            .setType(MethodType.UNARY)
            .setFullMethodName(fullMethodName)
            .build();
    interceptor.interceptCall(
        methodDescriptor, CallOptions.DEFAULT, channel);
    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getMaxOutboundMessageSize()).isEqualTo(6);

    fullMethodName =
        MethodDescriptor.generateFullMethodName("service2", "method");
    methodDescriptor =
        MethodDescriptor.newBuilder(new NoopMarshaller(), new NoopMarshaller())
            .setType(MethodType.UNARY)
            .setFullMethodName(fullMethodName)
            .build();
    interceptor.interceptCall(
        methodDescriptor, CallOptions.DEFAULT, channel);
    verify(channel).newCall(eq(methodDescriptor), callOptionsCap.capture());
    assertThat(callOptionsCap.getValue().getMaxOutboundMessageSize()).isEqualTo(5);
  }

  @Test
  public void methodInfo_validateDeadline() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "timeout", "10000000000000000s");

    thrown.expectMessage("Duration value is out of range");

    new MethodInfo(methodConfig, false, 1, 1);
  }

  @Test
  public void methodInfo_saturateDeadline() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "timeout", "315576000000s");

    MethodInfo info = new MethodInfo(methodConfig, false, 1, 1);

    assertThat(info.timeoutNanos).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void methodInfo_badMaxRequestSize() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "maxRequestMessageBytes", -1d);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("exceeds bounds");

    new MethodInfo(methodConfig, false, 1, 1);
  }

  @Test
  public void methodInfo_badMaxResponseSize() {
    JsonObj name = new JsonObj("service", "service");
    JsonObj methodConfig = new JsonObj("name", new JsonList(name), "maxResponseMessageBytes", -1d);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("exceeds bounds");

    new MethodInfo(methodConfig, false, 1, 1);
  }

  private static ManagedChannelServiceConfig createManagedChannelServiceConfig(
      JsonObj rawServiceConfig) {
    // current tests doesn't use any other values except rawServiceConfig, so provide dummy values.
    return ManagedChannelServiceConfig.fromServiceConfig(
        rawServiceConfig,
        /* retryEnabled= */ true,
        /* maxRetryAttemptsLimit= */ 3,
        /* maxHedgedAttemptsLimit= */ 4,
        /* loadBalancingConfig= */ null);
  }

  private static final class NoopMarshaller implements MethodDescriptor.Marshaller<Void> {

    @Override
    public InputStream stream(Void value) {
      return null;
    }

    @Override
    public Void parse(InputStream stream) {
      return null;
    }
  }
}
