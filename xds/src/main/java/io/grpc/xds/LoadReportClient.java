/*
 * Copyright 2019 The gRPC Authors
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

package io.grpc.xds;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.protobuf.util.Durations;
import io.envoyproxy.envoy.service.load_stats.v3.LoadReportingServiceGrpc;
import io.envoyproxy.envoy.service.load_stats.v3.LoadReportingServiceGrpc.LoadReportingServiceStub;
import io.envoyproxy.envoy.service.load_stats.v3.LoadStatsRequest;
import io.envoyproxy.envoy.service.load_stats.v3.LoadStatsResponse;
import io.grpc.InternalLogId;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.SynchronizationContext.ScheduledHandle;
import io.grpc.internal.BackoffPolicy;
import io.grpc.stub.StreamObserver;
import io.grpc.xds.EnvoyProtoData.ClusterStats;
import io.grpc.xds.EnvoyProtoData.Node;
import io.grpc.xds.XdsClient.XdsChannel;
import io.grpc.xds.XdsLogger.XdsLogLevel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Client of xDS load reporting service based on LRS protocol, which reports load stats of
 * gRPC client's perspective to a management server.
 */
@NotThreadSafe
final class LoadReportClient {
  @VisibleForTesting
  static final String TARGET_NAME_METADATA_KEY = "PROXYLESS_CLIENT_HOSTNAME";

  private final InternalLogId logId;
  private final XdsLogger logger;
  private final XdsChannel xdsChannel;
  private final Node node;
  private final SynchronizationContext syncContext;
  private final ScheduledExecutorService timerService;
  private final Stopwatch retryStopwatch;
  private final BackoffPolicy.Provider backoffPolicyProvider;
  private final LoadStatsManager loadStatsManager;

  private boolean started;

  @Nullable
  private BackoffPolicy lrsRpcRetryPolicy;
  @Nullable
  private ScheduledHandle lrsRpcRetryTimer;
  @Nullable
  private LrsStream lrsStream;

  LoadReportClient(
      String targetName,
      LoadStatsManager loadStatsManager,
      XdsChannel xdsChannel,
      Node node,
      SynchronizationContext syncContext,
      ScheduledExecutorService scheduledExecutorService,
      BackoffPolicy.Provider backoffPolicyProvider,
      Supplier<Stopwatch> stopwatchSupplier) {
    this.loadStatsManager = checkNotNull(loadStatsManager, "loadStatsManager");
    this.xdsChannel = checkNotNull(xdsChannel, "xdsChannel");
    this.syncContext = checkNotNull(syncContext, "syncContext");
    this.timerService = checkNotNull(scheduledExecutorService, "timeService");
    this.backoffPolicyProvider = checkNotNull(backoffPolicyProvider, "backoffPolicyProvider");
    checkNotNull(stopwatchSupplier, "stopwatchSupplier");
    this.retryStopwatch = stopwatchSupplier.get();
    checkNotNull(targetName, "targetName");
    checkNotNull(node, "node");
    Map<String, Object> newMetadata = new HashMap<>();
    if (node.getMetadata() != null) {
      newMetadata.putAll(node.getMetadata());
    }
    newMetadata.put(TARGET_NAME_METADATA_KEY, targetName);
    this.node = node.toBuilder().setMetadata(newMetadata).build();
    logId = InternalLogId.allocate("lrs-client", targetName);
    logger = XdsLogger.withLogId(logId);
    logger.log(XdsLogLevel.INFO, "Created");
  }

  /**
   * Establishes load reporting communication and negotiates with traffic director to report load
   * stats periodically. Calling this method on an already started {@link LoadReportClient} is
   * no-op.
   */
  void startLoadReporting() {
    if (started) {
      return;
    }
    started = true;
    logger.log(XdsLogLevel.INFO, "Starting load reporting RPC");
    startLrsRpc();
  }

  /**
   * Terminates load reporting. Calling this method on an already stopped
   * {@link LoadReportClient} is no-op.
   */
  void stopLoadReporting() {
    if (!started) {
      return;
    }
    logger.log(XdsLogLevel.INFO, "Stopping load reporting RPC");
    if (lrsRpcRetryTimer != null) {
      lrsRpcRetryTimer.cancel();
    }
    if (lrsStream != null) {
      lrsStream.close(Status.CANCELLED.withDescription("stop load reporting").asException());
    }
    started = false;
    // Do not shutdown channel as it is not owned by LrsClient.
  }

  @VisibleForTesting
  static class LoadReportingTask implements Runnable {
    private final LrsStream stream;

    LoadReportingTask(LrsStream stream) {
      this.stream = stream;
    }

    @Override
    public void run() {
      stream.sendLoadReport();
    }
  }

  @VisibleForTesting
  class LrsRpcRetryTask implements Runnable {

    @Override
    public void run() {
      startLrsRpc();
    }
  }

  private void startLrsRpc() {
    checkState(lrsStream == null, "previous lbStream has not been cleared yet");
    if (xdsChannel.isUseProtocolV3()) {
      lrsStream = new LrsStreamV3();
    } else {
      lrsStream = new LrsStreamV2();
    }
    retryStopwatch.reset().start();
    lrsStream.start();
  }

  private abstract class LrsStream {
    boolean initialResponseReceived;
    boolean closed;
    long loadReportIntervalNano = -1;
    boolean reportAllClusters;
    List<String> clusterNames;  // clusters to report loads for, if not report all.
    ScheduledHandle loadReportTimer;

    abstract void start();

    abstract void sendLoadStatsRequest(LoadStatsRequestData request);

    abstract void sendError(Exception error);

    final void handleResponse(final LoadStatsResponseData response) {
      syncContext.execute(new Runnable() {
        @Override
        public void run() {
          if (closed) {
            return;
          }
          if (!initialResponseReceived) {
            logger.log(XdsLogLevel.DEBUG, "Initial LRS response received");
            initialResponseReceived = true;
          }
          reportAllClusters = response.getSendAllClusters();
          if (reportAllClusters) {
            logger.log(XdsLogLevel.INFO, "Report loads for all clusters");
          } else {
            logger.log(XdsLogLevel.INFO, "Report loads for clusters: ", response.getClustersList());
            clusterNames = response.getClustersList();
          }
          long interval = response.getLoadReportingIntervalNanos();
          logger.log(XdsLogLevel.INFO, "Update load reporting interval to {0} ns", interval);
          loadReportIntervalNano = interval;
          scheduleNextLoadReport();
        }
      });
    }

    final void handleRpcError(final Throwable t) {
      syncContext.execute(new Runnable() {
        @Override
        public void run() {
          handleStreamClosed(Status.fromThrowable(t));
        }
      });
    }

    final void handleRpcComplete() {
      syncContext.execute(new Runnable() {
        @Override
        public void run() {
          handleStreamClosed(
              Status.UNAVAILABLE.withDescription("Closed by server"));
        }
      });
    }

    private void sendLoadReport() {
      List<ClusterStats> clusterStatsList;
      if (reportAllClusters) {
        clusterStatsList = loadStatsManager.getAllLoadReports();
      } else {
        clusterStatsList = new ArrayList<>();
        for (String name : clusterNames) {
          clusterStatsList.addAll(loadStatsManager.getClusterLoadReports(name));
        }
      }
      LoadStatsRequestData request = new LoadStatsRequestData(node, clusterStatsList);
      sendLoadStatsRequest(request);
      scheduleNextLoadReport();
    }

    private void scheduleNextLoadReport() {
      // Cancel pending load report and reschedule with updated load reporting interval.
      if (loadReportTimer != null && loadReportTimer.isPending()) {
        loadReportTimer.cancel();
        loadReportTimer = null;
      }
      if (loadReportIntervalNano > 0) {
        loadReportTimer = syncContext.schedule(
            new LoadReportingTask(this), loadReportIntervalNano, TimeUnit.NANOSECONDS,
            timerService);
      }
    }

    private void handleStreamClosed(Status status) {
      checkArgument(!status.isOk(), "unexpected OK status");
      if (closed) {
        return;
      }
      logger.log(
          XdsLogLevel.ERROR,
          "LRS stream closed with status {0}: {1}. Cause: {2}",
          status.getCode(), status.getDescription(), status.getCause());
      closed = true;
      cleanUp();

      long delayNanos = 0;
      if (initialResponseReceived || lrsRpcRetryPolicy == null) {
        // Reset the backoff sequence if balancer has sent the initial response, or backoff sequence
        // has never been initialized.
        lrsRpcRetryPolicy = backoffPolicyProvider.get();
      }
      // Backoff only when balancer wasn't working previously.
      if (!initialResponseReceived) {
        // The back-off policy determines the interval between consecutive RPC upstarts, thus the
        // actual delay may be smaller than the value from the back-off policy, or even negative,
        // depending how much time was spent in the previous RPC.
        delayNanos =
            lrsRpcRetryPolicy.nextBackoffNanos() - retryStopwatch.elapsed(TimeUnit.NANOSECONDS);
      }
      logger.log(XdsLogLevel.INFO, "Retry LRS stream in {0} ns", delayNanos);
      if (delayNanos <= 0) {
        startLrsRpc();
      } else {
        lrsRpcRetryTimer =
            syncContext.schedule(new LrsRpcRetryTask(), delayNanos, TimeUnit.NANOSECONDS,
                timerService);
      }
    }

    private void close(Exception error) {
      if (closed) {
        return;
      }
      closed = true;
      cleanUp();
      sendError(error);
    }

    private void cleanUp() {
      if (loadReportTimer != null) {
        loadReportTimer.cancel();
        loadReportTimer = null;
      }
      if (lrsStream == this) {
        lrsStream = null;
      }
    }
  }

  private final class LrsStreamV2 extends LrsStream {
    StreamObserver<io.envoyproxy.envoy.service.load_stats.v2.LoadStatsRequest> lrsRequestWriterV2;

    @Override
    void start() {
      StreamObserver<io.envoyproxy.envoy.service.load_stats.v2.LoadStatsResponse>
              lrsResponseReaderV2 =
          new StreamObserver<io.envoyproxy.envoy.service.load_stats.v2.LoadStatsResponse>() {
            @Override
            public void onNext(
                io.envoyproxy.envoy.service.load_stats.v2.LoadStatsResponse response) {
              logger.log(XdsLogLevel.DEBUG, "Received LRS response:\n{0}", response);
              handleResponse(LoadStatsResponseData.fromEnvoyProtoV2(response));
            }

            @Override
            public void onError(Throwable t) {
              handleRpcError(t);
            }

            @Override
            public void onCompleted() {
              handleRpcComplete();
            }
          };
      io.envoyproxy.envoy.service.load_stats.v2.LoadReportingServiceGrpc.LoadReportingServiceStub
          stubV2 = io.envoyproxy.envoy.service.load_stats.v2.LoadReportingServiceGrpc.newStub(
              xdsChannel.getManagedChannel());
      lrsRequestWriterV2 = stubV2.withWaitForReady().streamLoadStats(lrsResponseReaderV2);
      logger.log(XdsLogLevel.DEBUG, "Sending initial LRS request");
      sendLoadStatsRequest(new LoadStatsRequestData(node, null));
    }

    @Override
    void sendLoadStatsRequest(LoadStatsRequestData request) {
      io.envoyproxy.envoy.service.load_stats.v2.LoadStatsRequest requestProto =
          request.toEnvoyProtoV2();
      lrsRequestWriterV2.onNext(requestProto);
      logger.log(XdsLogLevel.DEBUG, "Sent LoadStatsRequest\n{0}", requestProto);
    }

    @Override
    void sendError(Exception error) {
      lrsRequestWriterV2.onError(error);
    }
  }

  private final class LrsStreamV3 extends LrsStream {
    StreamObserver<LoadStatsRequest> lrsRequestWriterV3;

    @Override
    void start() {
      StreamObserver<LoadStatsResponse> lrsResponseReaderV3 =
          new StreamObserver<LoadStatsResponse>() {
            @Override
            public void onNext(LoadStatsResponse response) {
              logger.log(XdsLogLevel.DEBUG, "Received LRS response:\n{0}", response);
              handleResponse(LoadStatsResponseData.fromEnvoyProtoV3(response));
            }

            @Override
            public void onError(Throwable t) {
              handleRpcError(t);
            }

            @Override
            public void onCompleted() {
              handleRpcComplete();
            }
          };
      LoadReportingServiceStub stubV3 =
          LoadReportingServiceGrpc.newStub(xdsChannel.getManagedChannel());
      lrsRequestWriterV3 = stubV3.withWaitForReady().streamLoadStats(lrsResponseReaderV3);
      logger.log(XdsLogLevel.DEBUG, "Sending initial LRS request");
      sendLoadStatsRequest(new LoadStatsRequestData(node, null));
    }

    @Override
    void sendLoadStatsRequest(LoadStatsRequestData request) {
      LoadStatsRequest requestProto = request.toEnvoyProtoV3();
      lrsRequestWriterV3.onNext(requestProto);
      logger.log(XdsLogLevel.DEBUG, "Sent LoadStatsRequest\n{0}", requestProto);
    }

    @Override
    void sendError(Exception error) {
      lrsRequestWriterV3.onError(error);
    }
  }

  private static final class LoadStatsRequestData {
    final Node node;
    @Nullable
    final List<ClusterStats> clusterStatsList;

    LoadStatsRequestData(Node node, @Nullable List<ClusterStats> clusterStatsList) {
      this.node = checkNotNull(node, "node");
      this.clusterStatsList = clusterStatsList;
    }

    io.envoyproxy.envoy.service.load_stats.v2.LoadStatsRequest toEnvoyProtoV2() {
      io.envoyproxy.envoy.service.load_stats.v2.LoadStatsRequest.Builder builder
          = io.envoyproxy.envoy.service.load_stats.v2.LoadStatsRequest.newBuilder()
              .setNode(node.toEnvoyProtoNodeV2());
      if (clusterStatsList != null) {
        for (ClusterStats stats : clusterStatsList) {
          builder.addClusterStats(stats.toEnvoyProtoClusterStatsV2());
        }
      }
      return builder.build();
    }

    LoadStatsRequest toEnvoyProtoV3() {
      LoadStatsRequest.Builder builder = LoadStatsRequest.newBuilder()
          .setNode(node.toEnvoyProtoNode());
      if (clusterStatsList != null) {
        for (ClusterStats stats : clusterStatsList) {
          builder.addClusterStats(stats.toEnvoyProtoClusterStats());
        }
      }
      return builder.build();
    }
  }

  private static final class LoadStatsResponseData {
    final boolean sendAllClusters;
    final List<String> clusters;
    final long loadReportingIntervalNanos;

    LoadStatsResponseData(
        boolean sendAllClusters, List<String> clusters, long loadReportingIntervalNanos) {
      this.sendAllClusters = sendAllClusters;
      this.clusters = checkNotNull(clusters, "clusters");
      this.loadReportingIntervalNanos = loadReportingIntervalNanos;
    }

    boolean getSendAllClusters() {
      return sendAllClusters;
    }

    List<String> getClustersList() {
      return clusters;
    }

    long getLoadReportingIntervalNanos() {
      return loadReportingIntervalNanos;
    }

    static LoadStatsResponseData fromEnvoyProtoV2(
        io.envoyproxy.envoy.service.load_stats.v2.LoadStatsResponse loadStatsResponse) {
      return new LoadStatsResponseData(
          loadStatsResponse.getSendAllClusters(),
          loadStatsResponse.getClustersList(),
          Durations.toNanos(loadStatsResponse.getLoadReportingInterval()));
    }

    static LoadStatsResponseData fromEnvoyProtoV3(LoadStatsResponse loadStatsResponse) {
      return new LoadStatsResponseData(
          loadStatsResponse.getSendAllClusters(),
          loadStatsResponse.getClustersList(),
          Durations.toNanos(loadStatsResponse.getLoadReportingInterval()));
    }
  }
}
