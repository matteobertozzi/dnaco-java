package tech.dnaco.storage;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gullivernet.server.http.HttpRouters.UriRoutesBuilder;
import com.gullivernet.server.http.direct.DirectMetricsHandler;
import com.gullivernet.server.http.direct.DirectTracesHandler;
import com.gullivernet.server.netty.dnaco.DnacoServer;
import com.gullivernet.server.netty.dnaco.DnacoServerConfig;
import com.gullivernet.server.netty.dnaco.packets.DnacoServicePacketHandler;
import com.gullivernet.server.netty.eloop.ServerEventLoop;
import com.gullivernet.server.netty.http.NettyHttpServer;
import com.gullivernet.server.netty.http.NettyHttpServerConfig;
import com.gullivernet.server.util.stats.ServerInfo;

import tech.dnaco.journal.JournalAsyncWriter;
import tech.dnaco.logging.LogAsyncWriter;
import tech.dnaco.logging.LogFileWriter;
import tech.dnaco.logging.LogUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.storage.demo.driver.RocksDbKvStore;
import tech.dnaco.storage.net.HttpStorageHandler;
import tech.dnaco.storage.net.StorageServiceHandler;
import tech.dnaco.storage.wal.Wal;
import tech.dnaco.storage.wal.WalFileWriter;
import tech.dnaco.telemetry.JvmMetrics;
import tech.dnaco.telemetry.TelemetryCollectorRegistry;
import tech.dnaco.util.BuildInfo;
import tech.dnaco.util.ShutdownUtil;
import tech.dnaco.util.ThreadUtil;

public final class Main {
  public static void main(final String[] args) throws Exception {
    // load service config from disk
    final StorageConfig conf = StorageConfig.INSTANCE;
    for (int i = 0; i < args.length; ++i) {
      if (args[i].startsWith("-conf=")) {
        conf.addJsonResource(args[i].substring(6));
      }
    }

    RocksDbKvStore.init(new File("STORAGE_DATA"), 64 << 20);

    // Setup Logger using ServiceConfig
    final LogAsyncWriter logWriter;
    Logger.setDefaultLevel(StorageConfig.INSTANCE.getLogLevel());
    if (conf.hasDiskLogger()) {
      logWriter = new LogAsyncWriter();
      logWriter.registerWriter(new LogFileWriter(conf.getLogsDir(), conf.getLogCleanerIntervalDays()));
      Logger.setWriter(logWriter);
      logWriter.start();
    } else {
      logWriter = null;
    }

    // load build info
    final BuildInfo buildInfo = BuildInfo.loadInfoFromManifest("dnaco-storage-server");
    JvmMetrics.INSTANCE.setBuildInfo(buildInfo);

    // Wal Writer Replay
    Wal.INSTANCE.replay();

    try (ServerEventLoop eventLoop = new ServerEventLoop(conf.getEloopBossGroups(), conf.getEloopWorkerGroups())) {
      final ServerInfo serverInfo = new ServerInfo()
          .setServiceUrl("goal.mobiledatacollection.it")
          .setProductName("dnaco-storage");

      // Setup the Storage Service
      final DnacoServer service = new DnacoServer(serverInfo);
      service.addListener(new DnacoServicePacketHandler(new StorageServiceHandler()));

      // Setup the HTTP Service
      final UriRoutesBuilder routes = new UriRoutesBuilder();
      routes.addHandler(new DirectTracesHandler());
      routes.addHandler(new DirectMetricsHandler());
      routes.addHandler(new HttpStorageHandler());

      final NettyHttpServer httpServer = new NettyHttpServer(serverInfo);
      httpServer.setHttpRoutes(routes, true);

      // Start the Storage Service
      service.start(eventLoop, new DnacoServerConfig().setPort(conf.getStorageServicePort()));

      // Start the HTTP Service
      httpServer.start(eventLoop, new NettyHttpServerConfig()
        .setCompressionEnabled(true)
        .setCorsEnabled(true)
        .setPort(conf.getStorageHttpPort()));

      // Setup the shutdown hook (clean ctrl+c shutdown)
      final AtomicBoolean running = new AtomicBoolean(true);
      ShutdownUtil.addShutdownHook("StorageService", running, service, httpServer);

      // Setup the WAL writer
      try (JournalAsyncWriter walWriter = new JournalAsyncWriter("StorageWal")) {
        Wal.INSTANCE.setWalWriter(walWriter);
        walWriter.registerWriter(new WalFileWriter());
        walWriter.start(conf.getStorageWalFlushIntervalMs());

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
      }

      // wait "forever" (until ctrl+c)
      service.waitStopSignal();
      httpServer.waitStopSignal();
    } catch (final Throwable e) {
      Logger.error(e, "uncaught exception");
    } finally {
      if (logWriter != null) {
        logWriter.close();
      }
    }
  }

  private static void metricsDump() {
    Logger.setSession(LoggerSession.newSession("metrics", "metrics", "metrics", Logger.getDefaultLevel(), LogUtil.nextTraceId()));
    try {
      Logger.raw(TelemetryCollectorRegistry.INSTANCE.humanReport());
    } catch (final Throwable e) {
      // no-op
    } finally {
      Logger.stopSession();
    }
  }
}
