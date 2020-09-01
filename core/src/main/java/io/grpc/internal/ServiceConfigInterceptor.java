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

import static com.google.common.base.Verify.verify;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Deadline;
import io.grpc.MethodDescriptor;
import io.grpc.internal.ManagedChannelServiceConfig.MethodInfo;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

/**
 * Modifies RPCs in conformance with a Service Config.
 */
final class ServiceConfigInterceptor implements ClientInterceptor {

  // Map from method name to MethodInfo
  @VisibleForTesting
  final AtomicReference<ManagedChannelServiceConfig> managedChannelServiceConfig =
      new AtomicReference<>();

  private final boolean retryEnabled;

  // Setting this to true and observing this equal to true are run in different threads.
  private volatile boolean initComplete;

  ServiceConfigInterceptor(boolean retryEnabled) {
    this.retryEnabled = retryEnabled;
  }

  void handleUpdate(@Nullable ManagedChannelServiceConfig serviceConfig) {
    managedChannelServiceConfig.set(serviceConfig);
    initComplete = true;
  }

  static final CallOptions.Key<RetryPolicy.Provider> RETRY_POLICY_KEY =
      CallOptions.Key.create("internal-retry-policy");
  static final CallOptions.Key<HedgingPolicy.Provider> HEDGING_POLICY_KEY =
      CallOptions.Key.create("internal-hedging-policy");

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      final MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    if (retryEnabled) {
      if (initComplete) {
        final RetryPolicy retryPolicy = getRetryPolicyFromConfig(method);
        final class ImmediateRetryPolicyProvider implements RetryPolicy.Provider {
          @Override
          public RetryPolicy get() {
            return retryPolicy;
          }
        }

        final HedgingPolicy hedgingPolicy = getHedgingPolicyFromConfig(method);
        final class ImmediateHedgingPolicyProvider implements HedgingPolicy.Provider {
          @Override
          public HedgingPolicy get() {
            return hedgingPolicy;
          }
        }

        verify(
            retryPolicy == null || hedgingPolicy == null,
            "Can not apply both retry and hedging policy for the method '%s'", method);

        callOptions = callOptions
            .withOption(RETRY_POLICY_KEY, new ImmediateRetryPolicyProvider())
            .withOption(HEDGING_POLICY_KEY, new ImmediateHedgingPolicyProvider());
      } else {
        final class DelayedRetryPolicyProvider implements RetryPolicy.Provider {
          /**
           * Returns RetryPolicy.DEFAULT if name resolving is not complete at the moment the method
           * is invoked, otherwise returns the RetryPolicy computed from service config.
           *
           * <p>Note that this method is used no more than once for each call.
           */
          @Override
          @Nullable
          public RetryPolicy get() {
            if (!initComplete) {
              return null;
            }
            return getRetryPolicyFromConfig(method);
          }
        }

        final class DelayedHedgingPolicyProvider implements HedgingPolicy.Provider {
          /**
           * Returns HedgingPolicy.DEFAULT if name resolving is not complete at the moment the
           * method is invoked, otherwise returns the HedgingPolicy computed from service config.
           *
           * <p>Note that this method is used no more than once for each call.
           */
          @Override
          @Nullable
          public HedgingPolicy get() {
            if (!initComplete) {
              return null;
            }
            HedgingPolicy hedgingPolicy = getHedgingPolicyFromConfig(method);
            verify(
                hedgingPolicy == null || getRetryPolicyFromConfig(method) == null,
                "Can not apply both retry and hedging policy for the method '%s'", method);
            return hedgingPolicy;
          }
        }

        callOptions = callOptions
            .withOption(RETRY_POLICY_KEY, new DelayedRetryPolicyProvider())
            .withOption(HEDGING_POLICY_KEY, new DelayedHedgingPolicyProvider());
      }
    }

    MethodInfo info = getMethodInfo(method);
    if (info == null) {
      return next.newCall(method, callOptions);
    }

    if (info.timeoutNanos != null) {
      Deadline newDeadline = Deadline.after(info.timeoutNanos, TimeUnit.NANOSECONDS);
      Deadline existingDeadline = callOptions.getDeadline();
      // If the new deadline is sooner than the existing deadline, swap them.
      if (existingDeadline == null || newDeadline.compareTo(existingDeadline) < 0) {
        callOptions = callOptions.withDeadline(newDeadline);
      }
    }
    if (info.waitForReady != null) {
      callOptions =
          info.waitForReady ? callOptions.withWaitForReady() : callOptions.withoutWaitForReady();
    }
    if (info.maxInboundMessageSize != null) {
      Integer existingLimit = callOptions.getMaxInboundMessageSize();
      if (existingLimit != null) {
        callOptions = callOptions.withMaxInboundMessageSize(
            Math.min(existingLimit, info.maxInboundMessageSize));
      } else {
        callOptions = callOptions.withMaxInboundMessageSize(info.maxInboundMessageSize);
      }
    }
    if (info.maxOutboundMessageSize != null) {
      Integer existingLimit = callOptions.getMaxOutboundMessageSize();
      if (existingLimit != null) {
        callOptions = callOptions.withMaxOutboundMessageSize(
            Math.min(existingLimit, info.maxOutboundMessageSize));
      } else {
        callOptions = callOptions.withMaxOutboundMessageSize(info.maxOutboundMessageSize);
      }
    }

    return next.newCall(method, callOptions);
  }

  @CheckForNull
  private MethodInfo getMethodInfo(MethodDescriptor<?, ?> method) {
    ManagedChannelServiceConfig mcsc = managedChannelServiceConfig.get();
    if (mcsc == null) {
      return null;
    }
    MethodInfo info;
    info = mcsc.getServiceMethodMap().get(method.getFullMethodName());
    if (info == null) {
      String serviceName = method.getServiceName();
      info = mcsc.getServiceMap().get(serviceName);
    }
    if (info == null) {
      info = mcsc.getDefaultMethodConfig();
    }
    return info;
  }

  @VisibleForTesting
  @Nullable
  RetryPolicy getRetryPolicyFromConfig(MethodDescriptor<?, ?> method) {
    MethodInfo info = getMethodInfo(method);
    return info == null ? null : info.retryPolicy;
  }

  @VisibleForTesting
  @Nullable
  HedgingPolicy getHedgingPolicyFromConfig(MethodDescriptor<?, ?> method) {
    MethodInfo info = getMethodInfo(method);
    return info == null ? null : info.hedgingPolicy;
  }
}
