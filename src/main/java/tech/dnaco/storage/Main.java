package tech.dnaco.storage;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gullivernet.server.http.HttpRouters.UriRoutesBuilder;
import com.gullivernet.server.http.direct.DirectMetricsHandler;
import com.gullivernet.server.http.direct.DirectTracesHandler;
import com.gullivernet.server.netty.eloop.ServerEventLoop;
import com.gullivernet.server.netty.http.NettyHttpServer;
import com.gullivernet.server.netty.http.NettyHttpServerConfig;
import com.gullivernet.server.util.stats.ServerInfo;

import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.logging.format.LogFileProvider;
import tech.dnaco.logging.format.LogFileWriter;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.rpc.DnacoRpcDispatcher;
import tech.dnaco.net.rpc.DnacoRpcObjectMapper;
import tech.dnaco.net.rpc.DnacoRpcService;
import tech.dnaco.storage.demo.EntityBackupScheduled;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.demo.logic.Storage;
import tech.dnaco.storage.net.EntityStorageHttpHandler;
import tech.dnaco.storage.net.EntityStorageRpcHandler;
import tech.dnaco.storage.net.EntityStorageScheduled;
import tech.dnaco.storage.net.EventStorageRpcHandler;
import tech.dnaco.telemetry.JvmMetrics;
import tech.dnaco.telemetry.TelemetryCollectorRegistry;
import tech.dnaco.threading.ShutdownUtil;
import tech.dnaco.threading.ThreadUtil;
import tech.dnaco.util.BuildInfo;

public final class Main {
  public static void main(final String[] args) throws Exception {
    // load service config from disk
    final StorageConfig conf = StorageConfig.INSTANCE;
    for (int i = 0; i < args.length; ++i) {
      if (args[i].startsWith("-conf=")) {
        conf.addJsonResource(args[i].substring(6));
      }
    }

    RocksDbKvStore.init(new File("STORAGE_DATA"), 512L << 20);

    // Setup Logger using ServiceConfig
    final LogFileProvider logWriter;
    Logger.setDefaultLevel(StorageConfig.INSTANCE.getLogLevel());
    if (conf.hasDiskLogger()) {
      logWriter = new LogFileProvider();
      logWriter.registerWriter(new LogFileWriter(conf.getLogsDir(), conf.getLogCleanerIntervalDays()));
      Logger.setProvider(logWriter);
      logWriter.start();
    } else {
      logWriter = null;
    }

    // load build info
    final BuildInfo buildInfo = BuildInfo.loadInfoFromManifest("dnaco-storage-server");
    JvmMetrics.INSTANCE.setBuildInfo(buildInfo);

    try (ServerEventLoop eventLoop = new ServerEventLoop(conf.getEloopBossGroups(), conf.getEloopWorkerGroups())) {
      try (ServiceEventLoop rpcEventLoop = new ServiceEventLoop(conf.getEloopBossGroups(), conf.getEloopWorkerGroups())) {
        final DnacoRpcDispatcher dispatcher = new DnacoRpcDispatcher(DnacoRpcObjectMapper.RPC_CBOR_OBJECT_MAPPER);
        dispatcher.addHandler(new EntityStorageRpcHandler());
        dispatcher.addHandler(new EventStorageRpcHandler());

        eventLoop.scheduleAtFixedRate(0, 1, TimeUnit.HOURS, new EntityStorageScheduled());
        eventLoop.scheduleAtFixedRate(0, 1, TimeUnit.DAYS, new EntityBackupScheduled());

        final ServerInfo serverInfo = new ServerInfo()
            .setServiceUrl("goal.mobiledatacollection.it")
            .setProductName("dnaco-storage");

        // Setup the Storage Service

        // Setup the HTTP Service
        final UriRoutesBuilder routes = new UriRoutesBuilder();
        routes.addHandler(new DirectTracesHandler());
        routes.addHandler(new DirectMetricsHandler());
        routes.addHandler(new EntityStorageHttpHandler());

        final NettyHttpServer httpServer = new NettyHttpServer(serverInfo);
        httpServer.setHttpRoutes(routes, true);

        // Start the Storage Service
        final DnacoRpcService service = new DnacoRpcService(dispatcher);
        service.bindTcpService(rpcEventLoop, conf.getStorageServicePort());

        // Start the HTTP Service
        httpServer.start(eventLoop, new NettyHttpServerConfig()
          .setCompressionEnabled(true)
          .setCorsEnabled(true)
          .setPort(conf.getStorageHttpPort()));

        // Setup the shutdown hook (clean ctrl+c shutdown)
        final AtomicBoolean running = new AtomicBoolean(true);
        ShutdownUtil.addShutdownHook("StorageService", running, service, httpServer);

        // main loop...
        long lastMetricsDump = System.nanoTime();
        while (running.get()) {
          final long now = System.nanoTime();
          if ((now - lastMetricsDump) > TimeUnit.MINUTES.toNanos(5)) {
            metricsDump();
            lastMetricsDump = now;
          }
          ThreadUtil.sleep(250);
        }

        // wait "forever" (until ctrl+c)
        service.waitStopSignal();
        httpServer.waitStopSignal();
      }
    } catch (final Throwable e) {
      Logger.error(e, "uncaught exception");
    } finally {
      Storage.shutdown();
      if (logWriter != null) {
        logWriter.close();
      }
    }
  }

  private static void metricsDump() {
    Logger.setSession(LoggerSession.newSession("metrics", "metrics", "metrics"));
    try {
      Logger.raw(TelemetryCollectorRegistry.INSTANCE.humanReport());
    } catch (final Throwable e) {
      // no-op
    } finally {
      Logger.stopSession();
    }
  }
}
