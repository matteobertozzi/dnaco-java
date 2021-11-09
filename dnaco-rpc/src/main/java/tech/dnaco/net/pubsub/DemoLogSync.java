package tech.dnaco.net.pubsub;

import java.io.File;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.journal.JournalAsyncWriter;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.pubsub.LogFileUtil.LogSyncMessage;
import tech.dnaco.net.pubsub.LogFileUtil.LogSyncMessageWriter;
import tech.dnaco.net.pubsub.LogFileUtil.LogWriter;
import tech.dnaco.net.pubsub.LogFileUtil.LogsStorage;
import tech.dnaco.net.pubsub.LogFileUtil.LogsTrackerManager;
import tech.dnaco.net.pubsub.LogSyncService.LogSyncServiceStoreHandler;
import tech.dnaco.threading.ThreadUtil;
import tech.dnaco.time.RetryUtil;
import tech.dnaco.util.RandData;

public class DemoLogSync {
  public static void main(final String[] args) throws Exception {
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
      localStorageTracker.registerNewTopicListener(tracker -> {
        try {
          tracker.setOffset(tracker.getMaxOffset());
          client.add(tracker);
        } catch (final Throwable e) {
          Logger.error(e, "uncaught exception during tracker registration: {}", tracker);
        }
      });
      client.connect("127.0.0.1", 57025);
      while (!client.isConnected()) Thread.yield();
      client.setReady();

      System.out.println("READY");
      if (true) {
        final JournalAsyncWriter<LogSyncMessage> journal = new JournalAsyncWriter<>("pubsub", new LogSyncMessageWriter());
        journal.registerWriter(new LogWriter(localStorageTracker::get));
        journal.start(100);

        for (int i = 0; i < 10000; ++i) {
          final byte[] data = RandData.generateBytes((20 + i) % 20);
          journal.addToLogQueue(Thread.currentThread(), new LogSyncMessage("topic-" + (i % 3), data));
          if ((i + 1) % 1000 == 0) ThreadUtil.sleep(2, TimeUnit.SECONDS);
        }
        journal.stop();
        journal.close();
      }

      ThreadUtil.sleep(10, TimeUnit.SECONDS);

      final MessageDigest digest = MessageDigest.getInstance("SHA-512");
      for (int i = 0; i < TOPICS.length; ++i) {
        digest.reset();
        File[] files = localStorage.getTopicDir(TOPICS[i]).listFiles();
        Arrays.sort(files, (a, b) -> a.getName().compareTo(b.getName()));
        for (final File f: files) {
          digest.update(Files.readAllBytes(f.toPath()));
        }
        System.out.println("local: " + BytesUtil.toHexString(digest.digest()));

        digest.reset();
        files = remoteStorage.getTopicDir(TOPICS[i]).listFiles();
        Arrays.sort(files, (a, b) -> a.getName().compareTo(b.getName()));
        for (final File f: files) {
          digest.update(Files.readAllBytes(f.toPath()));
        }
        System.out.println("remot: " + BytesUtil.toHexString(digest.digest()));
      }

      service.waitStopSignal();
      client.disconnect();
      client.waitForShutdown();
    }
  }
}
