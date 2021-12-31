package tech.dnaco.net.logsync;

import java.io.File;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.journal.JournalAsyncWriter;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.logsync.LogFileUtil.LogSyncMessage;
import tech.dnaco.net.logsync.LogFileUtil.LogWriter;
import tech.dnaco.net.logsync.LogFileUtil.LogsConsumer;
import tech.dnaco.net.logsync.LogFileUtil.LogsEventListener;
import tech.dnaco.net.logsync.LogFileUtil.LogsFileTracker;
import tech.dnaco.net.logsync.LogFileUtil.LogsStorage;
import tech.dnaco.net.logsync.LogFileUtil.LogsTrackerManager;
import tech.dnaco.net.logsync.LogFileUtil.SimpleLogEntryReader;
import tech.dnaco.net.logsync.LogSyncService.LogSyncServiceStoreHandler;
import tech.dnaco.threading.ThreadUtil;
import tech.dnaco.time.RetryUtil;

public class DemoLogSync {
  public static void main(final String[] args) throws Exception {
    Logger.setDefaultLevel(LogLevel.WARNING);
    final String[] TOPICS = new String[] { "topic-0", "topic-1", "topic-2" };

    try (final ServiceEventLoop eventLoop = new ServiceEventLoop(1, 1)) {
      // bind log-sync service
      final LogsStorage remoteStorage = new LogsStorage(new File("demo-logsync/remote-storage"));
      final LogsTrackerManager remoteLogsTracker = new LogsTrackerManager(remoteStorage);
      final LogSyncService service = new LogSyncService();
      service.registerListener(new LogSyncServiceStoreHandler(remoteLogsTracker::get));
      service.bindTcpService(eventLoop, 57025);

      final LogsStorage localStorage = new LogsStorage(new File("demo-logsync/local-storage"));
      final LogsTrackerManager localStorageTracker = new LogsTrackerManager(localStorage);
      final LogSyncClient client = LogSyncClient.newTcpClient(eventLoop, RetryUtil.newFixedRetry(1000));

      if (false) {
        checksumDirs(localStorage, remoteStorage);
        return;
      }

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

      System.out.println("READY");
      if (true) {
        final JournalAsyncWriter<LogSyncMessage> journal = new JournalAsyncWriter<>("logs-sync", LogFileUtil.LOG_SYNC_MESSAGE_JOURNAL_SUPPLIER);
        journal.registerWriter(new LogWriter(localStorageTracker::get));
        journal.start(100);

        final long startTime = System.nanoTime();
        long lastReport = startTime;
        for (long i = 0; (System.nanoTime() - startTime) < TimeUnit.MINUTES.toNanos(5); ++i) {
          final String logId = "topic-" + (i % (TOPICS.length - (i > 20_000 ? 1 : 0)));
          final byte[] data = ("event-" + i).getBytes();
          journal.addToLogQueue(Thread.currentThread(), new LogSyncMessage(logId, data));
          ThreadUtil.sleep(10 + Math.round(Math.random() * 1000), TimeUnit.MICROSECONDS);

          if ((System.nanoTime() - lastReport) > TimeUnit.SECONDS.toNanos(1)) {
            lastReport = System.nanoTime();
            System.out.println("\n".repeat(40));
            System.out.println("local storage");
            for (final String logsId: localStorageTracker.getLogsIds()) {
              final LogsFileTracker tracker = localStorageTracker.get(logsId);
              tracker.cleanupAllFiles(Duration.ofSeconds(1), tracker.getGatingSequence());
              System.out.println(" -> " + logsId + " -> "
                + tracker.getGatingSequence() + "/" + tracker.getMaxOffset()
                + " -> " + Arrays.toString(localStorage.getLogsDir(logsId).list()));
            }
            System.out.println("remote storage");
            for (final String logsId: remoteLogsTracker.getLogsIds()) {
              final LogsFileTracker tracker = remoteLogsTracker.get(logsId);
              tracker.cleanupAllFiles(Duration.ofSeconds(1), tracker.getGatingSequence());
              System.out.println(" -> " + logsId + " -> "
                + tracker.getGatingSequence() + "/" + tracker.getMaxOffset()
                + " -> " + Arrays.toString(remoteStorage.getLogsDir(logsId).list()));
            }
          }


          if (false && (i + 1) % 1000 == 0) {
            for (int c = 0; c < 1; ++c) {
              localStorageTracker.get(logId).cleanupFiles(Duration.ofSeconds(5));
              ThreadUtil.sleep(2, TimeUnit.SECONDS);
            }
          }
        }
        journal.stop();
        journal.close();
      }

      ThreadUtil.sleep(10, TimeUnit.SECONDS);

      //checksumDirs(localStorage, remoteStorage);

      service.waitStopSignal();
      client.disconnect();
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
  }

  private static byte[] checksum(final LogsConsumer consumer) throws Exception {
    final MessageDigest digest = MessageDigest.getInstance("SHA-512");
    long consumed = 0;
    while (consumer.hasMore()) {
      consumed += SimpleLogEntryReader.read(consumer, Duration.ofSeconds(1).toNanos(), data -> {
        digest.update(data.rawBuffer(), data.offset(), data.length());
      });
    }
    if (consumed != consumer.getMaxOffset()) {
      throw new IllegalArgumentException(consumer + " expected to be fully consumed: " + consumed + "/" + consumer.getMaxOffset());
    }
    return digest.digest();
  }
}
