package tech.dnaco.net.logsync;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.LongValue;
import tech.dnaco.journal.JournalAsyncWriter;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.logsync.LogFileUtil.LogSyncMessage;
import tech.dnaco.net.logsync.LogFileUtil.LogWriter;
import tech.dnaco.net.logsync.LogFileUtil.LogsEventListener;
import tech.dnaco.net.logsync.LogFileUtil.LogsStorage;
import tech.dnaco.net.logsync.LogFileUtil.SimpleLogEntryReader;
import tech.dnaco.net.logsync.LogSyncService.LogSyncServiceStoreHandler;
import tech.dnaco.threading.ShutdownUtil;
import tech.dnaco.threading.ThreadUtil;
import tech.dnaco.time.RetryUtil;

public class DemoLogSync2 {
  private static final String[] TOPICS = new String[] { "topic-0" };

  private static void remoteSyncService() throws Exception {
    Logger.setDefaultLevel(LogLevel.WARNING);

    try (final ServiceEventLoop eventLoop = new ServiceEventLoop(1, 1)) {
      final LogsStorage remoteStorage = new LogsStorage(new File("demo-logsync/remote-storage"));
      final LogsTrackerManager remoteLogsTracker = new LogsTrackerManager(remoteStorage);

      final LogSyncService service = new LogSyncService();
      service.registerListener(new LogSyncServiceStoreHandler(remoteLogsTracker::get));
      service.bindTcpService(eventLoop, 57025);

      service.waitStopSignal();
    }
  }

  private static void localSyncClient(final long startOffset) throws Exception {
    try (final ServiceEventLoop eventLoop = new ServiceEventLoop(1, 1)) {
      final LogsStorage localStorage = new LogsStorage(new File("demo-logsync/local-storage"));
      final LogsTrackerManager localStorageTracker = new LogsTrackerManager(localStorage);
      final LogSyncClient client = LogSyncClient.newTcpClient(eventLoop, RetryUtil.newFixedRetry(1000));
      client.registerOffsetStore((logsId, offset) -> {
        Files.write(Path.of("demo-logsync/client." + logsId), String.valueOf(offset).getBytes());
      });

      // load logs already on disk
      for (final String logsId: localStorageTracker.getLogsIds()) {
        final LogsFileTracker tracker = localStorageTracker.get(logsId);
        final long offset = tracker.getMaxOffset();
        if (tracker.cleanupAllFiles(Duration.ofSeconds(5), offset)) {
          Logger.debug("{} all files removed", logsId);
          continue;
        }
        // NOTE: remove from offset store and add to the log-sync client
        client.add(tracker.newConsumer("log-sync-consumer", offset));
      }

      // listen for new logs
      localStorageTracker.registerLogsEventListener(new LogsEventListener() {
        @Override
        public void newLogEvent(final LogsFileTracker tracker) {
          final LogsConsumer consumer = tracker.newConsumer("log-sync-consumer", 0);
          client.add(consumer);
        }

        @Override
        public void removeLogEvent(final LogsFileTracker tracker) {
          for (final LogsConsumer consumer: tracker.getConsumers()) {
            client.remove(consumer);
          }
          tracker.removeAllConsumers();
        }
      });

      // start log-sync client
      client.connect("127.0.0.1", 57025);
      while (!client.isConnected()) Thread.yield();
      client.setReady();
      System.out.println("CLIENT READY");

      final JournalAsyncWriter<LogSyncMessage> journal = new JournalAsyncWriter<>("logs-sync", LogFileUtil.LOG_SYNC_MESSAGE_JOURNAL_SUPPLIER);
      journal.registerWriter(new LogWriter(localStorageTracker::get));
      journal.start(250);

      final AtomicBoolean running = new AtomicBoolean(true);
      ShutdownUtil.addShutdownHook("shutdown", running);

      final byte[] buf9 = new byte[9];
      long v = startOffset;
      for (long i = 0; running.get(); ++i) {
        final String logId = "topic-" + (i % TOPICS.length);
        final int n = VarInt.write(buf9, ++v);
        journal.addToLogQueue(Thread.currentThread(), new LogSyncMessage(logId, buf9, 0, n));
        ThreadUtil.sleep(75);
      }
      System.out.println("LAST VALUE: " + v);

      journal.stop();
      journal.close();
      client.sendStopSignal();
      client.waitForShutdown();
    }
  }

  private static void checksumDirs(final LogsStorage localStorage, final LogsStorage remoteStorage) throws Exception {
    final LogsTrackerManager remoteLogsTracker = new LogsTrackerManager(remoteStorage);
    final LogsTrackerManager localStorageTracker = new LogsTrackerManager(localStorage);

    for (final String logsId: localStorageTracker.getLogsIds()) {
      final LogsConsumer localConsumer = localStorageTracker.get(logsId).newConsumer("local-consumer", 0);
      byte[] checksum = checksum(localConsumer);
      System.out.println("local: " + logsId + " -> " + BytesUtil.toHexString(checksum));

      final LogsConsumer remoteConsumer = remoteLogsTracker.get(logsId).newConsumer("remote-consumer", 0);
      checksum = checksum(remoteConsumer);
      System.out.println("remot: " + logsId + " -> " + BytesUtil.toHexString(checksum));
    }

    for (final String logsId: localStorageTracker.getLogsIds()) {
      final LogsConsumer remoteConsumer = remoteLogsTracker.get(logsId).newConsumer("remote-consumer", 0);
      final LongValue value = new LongValue();
      final LongValue lastValue = new LongValue();
      while (remoteConsumer.hasMore()) {
        final long consumed = SimpleLogEntryReader.read(remoteConsumer, Duration.ofSeconds(1).toNanos(), data -> {
          //System.out.println(data.length() + " -> " + BytesUtil.toHexString(data.rawBuffer(), data.offset(), data.length()));
          final int n = VarInt.read(data, value);
          System.out.println(value.get() + " -> " + (value.get() - lastValue.get()));
          lastValue.set(value.get());
        });
        remoteConsumer.consume(consumed);
      }
    }
  }

  private static byte[] checksum(final LogsConsumer consumer) throws Exception {
    final MessageDigest digest = MessageDigest.getInstance("SHA-512");
    long totalConsumed = 0;
    while (consumer.hasMore()) {
      final long consumed = SimpleLogEntryReader.read(consumer, Duration.ofSeconds(1).toNanos(), data -> {
        digest.update(data.rawBuffer(), data.offset(), data.length());
      });
      consumer.consume(consumed);
      totalConsumed += consumed;
    }
    if (totalConsumed != consumer.getMaxOffset()) {
      throw new IllegalArgumentException(consumer + " expected to be fully consumed: " + totalConsumed + "/" + consumer.getMaxOffset());
    }
    return digest.digest();
  }

  public static void main(final String[] args) throws Exception {
    enum Exec { SERVICE, CLIENT, CHECK }
    Exec exec = null;
    long offset = 0;
    for (int i = 0; i < args.length; ++i) {
      switch (args[i]) {
        case "server" -> exec = Exec.SERVICE;
        case "client" -> exec = Exec.CLIENT;
        case "check" -> exec = Exec.CHECK;
        case "offset" -> offset = Long.parseLong(args[++i]);
      }
    }

    if (exec == Exec.SERVICE) {
      remoteSyncService();
    } else if (exec == Exec.CLIENT) {
      localSyncClient(offset);
    } else if (exec == Exec.CHECK) {
      final LogsStorage remoteStorage = new LogsStorage(new File("demo-logsync/remote-storage"));
      final LogsStorage localStorage = new LogsStorage(new File("demo-logsync/local-storage"));
      checksumDirs(localStorage, remoteStorage);
    }
  }
}
