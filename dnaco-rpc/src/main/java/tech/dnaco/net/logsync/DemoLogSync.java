package tech.dnaco.net.logsync;

import java.io.File;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.journal.JournalAsyncWriter;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.logsync.LogFileUtil.LogSyncMessage;
import tech.dnaco.net.logsync.LogFileUtil.LogSyncMessageWriter;
import tech.dnaco.net.logsync.LogFileUtil.LogWriter;
import tech.dnaco.net.logsync.LogFileUtil.LogsConsumer;
import tech.dnaco.net.logsync.LogFileUtil.LogsFileTracker;
import tech.dnaco.net.logsync.LogFileUtil.LogsStorage;
import tech.dnaco.net.logsync.LogFileUtil.LogsTrackerManager;
import tech.dnaco.net.logsync.LogFileUtil.SimpleLogEntryReader;
import tech.dnaco.net.logsync.LogSyncService.LogSyncServiceStoreHandler;
import tech.dnaco.threading.ThreadUtil;
import tech.dnaco.time.RetryUtil;
import tech.dnaco.util.RandData;

public class DemoLogSync {
  public static void main(final String[] args) throws Exception {
    Logger.setDefaultLevel(LogLevel.INFO);
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

      // load logs already on disk
      for (final String logsId: localStorageTracker.getLogsIds()) {
        final LogsFileTracker tracker = localStorageTracker.get(logsId);
        if (tracker.cleanupFiles(Duration.ofHours(5))) {
          Logger.debug("{} all files removed", logsId);
          continue;
        }
        // NOTE: remove from offset store and add to the log-sync client
        client.add(tracker.newConsumer("log-sync-consumer", tracker.getMaxOffset()));
      }

      // listen for new logs
      localStorageTracker.registerNewLogsListener(tracker -> {
        try {
          final LogsConsumer consumer = tracker.newConsumer("log-sync-consumer", 0);
          client.add(consumer);
        } catch (final Throwable e) {
          Logger.error(e, "uncaught exception during tracker registration: {}", tracker);
        }
      });

      // start log-sync client
      client.connect("127.0.0.1", 57025);
      while (!client.isConnected()) Thread.yield();
      client.setReady();

      System.out.println("READY");
      if (true) {
        final JournalAsyncWriter<LogSyncMessage> journal = new JournalAsyncWriter<>("logs-sync", new LogSyncMessageWriter());
        journal.registerWriter(new LogWriter(localStorageTracker::get));
        journal.start(100);

        for (int i = 0; i < 100_000; ++i) {
          final String logId = "topic-" + (i % TOPICS.length);
          final byte[] data = RandData.generateBytes(1024);
          journal.addToLogQueue(Thread.currentThread(), new LogSyncMessage(logId, data));
          if ((i + 1) % 1000 == 0) {
            //localStorageTracker.get(logId).cleanupFiles(Duration.ofSeconds(10));
            ThreadUtil.sleep(500, TimeUnit.MILLISECONDS);
          }
        }
        journal.stop();
        journal.close();
      }

      ThreadUtil.sleep(10, TimeUnit.SECONDS);

      for (int i = 0; i < TOPICS.length; ++i) {
        byte[] checksum = checksumDir(localStorage.getLogsDir(TOPICS[i]));
        System.out.println("local: " + BytesUtil.toHexString(checksum));

        checksum = checksumDir(remoteStorage.getLogsDir(TOPICS[i]));
        System.out.println("remot: " + BytesUtil.toHexString(checksum));
      }

      service.waitStopSignal();
      client.disconnect();
      client.waitForShutdown();
    }
  }

  private static byte[] checksumDir(final File dir) throws Exception {
    final MessageDigest digest = MessageDigest.getInstance("SHA-512");
    final File[] files = dir.listFiles();
    if (ArrayUtil.isNotEmpty(files)) {
      Arrays.sort(files, (a, b) -> a.getName().compareTo(b.getName()));
      for (final File f: files) {
        //digest.update(Files.readAllBytes(f.toPath()));
        final long consumed = SimpleLogEntryReader.read(f, 0, f.length(), data -> {
          digest.update(data.rawBuffer(), data.offset(), data.length());
        });
        if (consumed != f.length()) {
          throw new IllegalArgumentException(f + " expected to be fully consumed: " + consumed + "/" + f.length());
        }
      }
    }
    return digest.digest();
  }
}
