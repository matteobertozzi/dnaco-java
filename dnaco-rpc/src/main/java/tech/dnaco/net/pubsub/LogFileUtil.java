package tech.dnaco.net.pubsub;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import tech.dnaco.bytes.ByteArrayReader;
import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.collections.arrays.paged.PagedByteArrayWriter;
import tech.dnaco.data.CborFormat;
import tech.dnaco.data.compression.ZstdUtil;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.io.IOUtil;
import tech.dnaco.io.LimitedInputStream;
import tech.dnaco.journal.JournalAsyncWriter;
import tech.dnaco.journal.JournalAsyncWriter.JournalEntryWriter;
import tech.dnaco.journal.JournalBuffer;
import tech.dnaco.journal.JournalEntry;
import tech.dnaco.journal.JournalWriter;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;

public final class LogFileUtil {
  private LogFileUtil() {
    // no-op
  }

  public static String newFileName(final long offset) {
    return String.format("%020d.%s", offset, HumansUtil.toHumanTs(ZonedDateTime.now()));
  }

  public static long offsetFromFileName(final String name) {
    final int offsetEof = name.indexOf('.');
    return Long.parseLong(name, 0, offsetEof, 10);
  }

  @FunctionalInterface
  public interface LogsTrackerSupplier {
    LogsTracker getLogsTracker(String groupId);
  }

  @FunctionalInterface
  public interface LogsTrackerConsumer {
    void accept(LogsTracker tracker);
  }

  @FunctionalInterface
  public interface LogOffsetStore {
    void store(LogsTracker tracker, long offset) throws Exception;
  }

  public static class LogsTrackerManager {
    private final CopyOnWriteArrayList<LogsTrackerConsumer> newTopicListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, LogsTracker> trackers = new ConcurrentHashMap<>();
    private final LogsStorage storage;

    public LogsTrackerManager(final LogsStorage storage) {
      this.storage = storage;
    }

    public Set<String> getLogKeys() {
      return storage.getTopics();
    }

    public LogsTrackerManager registerNewTopicListener(final LogsTrackerConsumer consumer) {
      this.newTopicListeners.add(consumer);
      return this;
    }

    public LogsTracker get(final String topic) {
      LogsTracker tracker = trackers.get(topic);
      if (tracker != null) return tracker;

      synchronized (this) {
        tracker = trackers.get(topic);
        if (tracker != null) return tracker;

        tracker = newTracker(topic);
        trackers.put(topic, tracker);
      }

      for (final LogsTrackerConsumer listener: newTopicListeners) {
        listener.accept(tracker);
      }
      return tracker;
    }

    private LogsTracker newTracker(final String topic) {
      final LogsTracker tracker = new LogsTracker(storage.getTopicDir(topic));
      tracker.loadFiles();
      return tracker;
    }
  }

  public static class LogWriter implements JournalWriter<LogSyncMessage> {
    private static final long ROLL_SIZE = 32 << 20;

    private final LogsTrackerSupplier logsTrackerSupplier;

    public LogWriter(final LogsTrackerSupplier logsTrackerSupplier) {
      this.logsTrackerSupplier = logsTrackerSupplier;
    }

    @Override
    public void manageOldLogs() {
      // no-op
    }

    @Override
    public void writeBuffers(final String groupId, final List<JournalBuffer<LogSyncMessage>> buffers) {
      final LogsTracker logsTracker = logsTrackerSupplier.getLogsTracker(groupId);

      File logFile = logsTracker.getLastBlockFile();
      if (logFile == null || logFile.length() > ROLL_SIZE) {
        Logger.debug("roll logFile:{} logSize:{}", logFile, logFile != null ? logFile.length() : 0);
        logFile = logsTracker.addNewFile();
      }

      logFile.getParentFile().mkdirs();
      long flushSize = 0;
      //final long startTimeNs = System.nanoTime();
      try (final FileOutputStream stream = new FileOutputStream(logFile, true)) {
        final byte[] block = writeBlock(groupId, buffers);
        final ByteArraySlice zblock = ZstdUtil.compress(block, 0, block.length);

        // +-------------+---------+----------+
        // | -- -- -- -- | blk len | zblk len |
        // +-------------+---------+----------+
        final int blkLenBytes = IntUtil.size(block.length);
        final int zblkLenBytes = IntUtil.size(zblock.length());
        stream.write((blkLenBytes - 1) << 2 | (zblkLenBytes - 1));
        IntEncoder.BIG_ENDIAN.writeFixed(stream, block.length, blkLenBytes);
        IntEncoder.BIG_ENDIAN.writeFixed(stream, zblock.length(), zblkLenBytes);
        zblock.writeTo(stream);
        stream.flush();

        flushSize += 1 + blkLenBytes + zblkLenBytes + zblock.length();
      } catch (final Throwable e) {
        Logger.logToStderr(LogLevel.ERROR, e, "unable to flush logs for groupId: {}", groupId);
      } finally {
        Logger.debug("flush data: {} {}", logFile, flushSize);
        logsTracker.addData(flushSize);
        //stats.addFlush(flushSize, System.nanoTime() - startTimeNs);
      }
    }

    private byte[] writeBlock(final String groupId, final List<JournalBuffer<LogSyncMessage>> buffers) throws IOException {
      try (PagedByteArrayWriter blockWriter = new PagedByteArrayWriter(1 << 20)) {
        try (final SimpleLogEntryWriter entryWriter = new SimpleLogEntryWriter(blockWriter)) {
          entryWriter.newBlock();
          for (final JournalBuffer<LogSyncMessage> threadBuf: buffers) {
            if (!threadBuf.hasGroupId(groupId)) continue;

            entryWriter.reset(threadBuf.getThread());
            threadBuf.process(groupId, entryWriter::add);
          }
        }

        return blockWriter.toByteArray();
      }
    }
  }

  public static class SimpleLogEntryWriter implements Closeable {
    private final OutputStream stream;

    public SimpleLogEntryWriter(final OutputStream stream) {
      this.stream = stream;
    }

    @Override
    public void close() throws IOException {
      // no-op
    }

    public void newBlock() throws IOException {
      // no-op
    }

    public void reset(final Thread thread) {
      // no-op
    }

    public void add(final PagedByteArray buf, final int offset) throws IOException {
      final int length = buf.getFixed32(offset);
      IntEncoder.writeUnsignedVarLong(stream, length);
      for (int i = 0; i < length; ++i) {
        stream.write(buf.get(offset + 4 + i));
      }
    }
  }

  @FunctionalInterface
  public interface LogEntryProcessor {
    void process(ByteArraySlice data) throws Exception;
  }

  public static class SimpleLogEntryReader {
    public static void read(final File logFile, final long offset, final long avail,
        final LogEntryProcessor processor) throws Exception {
      try (final InputStream stream = new LimitedInputStream(new FileInputStream(logFile), offset + avail)) {
        stream.skipNBytes(offset);

        while (stream.available() > 0) {
          final byte[] block = readBlock(stream);
          try (ByteArrayReader blockReader = new ByteArrayReader(block)) {
            while (blockReader.available() > 0) {
              final int length = IntDecoder.readUnsignedVarInt(blockReader);
              final ByteArraySlice data = new ByteArraySlice(block, blockReader.readOffset(), length);
              processor.process(data);
              blockReader.skip(length);
            }
          }
        }
      }
    }

    private static byte[] readBlock(final InputStream stream) throws IOException {
      // +-------------+---------+----------+
      // | -- -- -- -- | blk len | zblk len |
      // +-------------+---------+----------+
      final int head = stream.read();
      if (head < 0) throw new EOFException();

      final int blkLenBytes = 1 + ((head >> 2) & 3);
      final int zblkLenBytes = 1 + (head & 3);
      final int length = (int) IntDecoder.BIG_ENDIAN.readFixed(stream, blkLenBytes);
      final int zLength = (int) IntDecoder.BIG_ENDIAN.readFixed(stream, zblkLenBytes);
      final byte[] zblock = IOUtil.readNBytes(stream, zLength);
      final byte[] block = new byte[length];
      if (ZstdUtil.decompress(block, zblock, 0, zLength) != length) {
        throw new IOException("unexpected zstd decompress length");
      }
      return block;
    }
  }

  public static class LogsStorage {
    private final File logsDir;

    public LogsStorage(final File logsDir) {
      this.logsDir = logsDir;
    }

    public Set<String> getTopics() {
      final String[] topics = logsDir.list();
      return topics != null ? Set.of(topics) : Set.of();
    }

    public File getTopicDir(final String name) {
      return new File(logsDir, name);
    }
  }

  public static class LogsTracker {
    private final ArrayList<Consumer<LogsTracker>> dataPublishedNotifier = new ArrayList<>();
    private final ArrayList<String> fileNames = new ArrayList<>();
    private final File logsDir;

    private long offset;
    private long blkOffset;
    private long nextOffset;
    private long maxOffset;

    public LogsTracker(final File logsDir) {
      this.logsDir = logsDir;
      this.maxOffset = 0;
      this.nextOffset = 0;
    }

    public String getName() {
      return logsDir.getName();
    }

    public synchronized LogsTracker loadFiles() {
      final String[] files = logsDir.list();
      if (ArrayUtil.isEmpty(files)) {
        Logger.trace("no log file available for dir: {}", logsDir);
        this.fileNames.clear();
        this.offset = 0;
        this.nextOffset = 0;
        this.maxOffset = 0;
        return this;
      }

      // logs name are in the format of <OFFSET>.<not-imporant>
      // so they will get sorted by offset.
      Arrays.sort(files);
      fileNames.clear();
      fileNames.addAll(List.of(files));

      // calculate max offset
      final String lastFileName = files[files.length - 1];
      final File lastFile = new File(logsDir, lastFileName);
      final long lastOffset = offsetFromFileName(lastFileName);
      final long lastSize = lastFile.length();
      this.maxOffset = lastOffset + lastSize;
      Logger.debug("found {} logs, last offset {} size {}", files.length, lastOffset, lastSize);

      // calculate offset & next offset
      this.offset = offsetFromFileName(files[0]);
      this.nextOffset = (files.length > 1) ? offsetFromFileName(files[1]) : maxOffset;
      return this;
    }

    public synchronized long getMaxOffset() {
      return maxOffset;
    }

    public synchronized long getOffset() {
      return offset;
    }

    public synchronized void setOffset(final long newOffset) throws IOException {
      // check if the offset is the tail
      if (newOffset == maxOffset) {
        this.offset = newOffset;
        this.nextOffset = newOffset;
        this.fileNames.clear();
        Logger.debug("{} new offset set to tail {}/{}", getName(), offset, nextOffset);
        return;
      }

      if (newOffset > maxOffset) {
        throw new IOException(getName() + " invalid offset " + newOffset + ", max offset is " + maxOffset);
      }

      int fileCount = fileNames.size();
      while (fileCount > 1) {
        final long nextOffset = offsetFromFileName(fileNames.get(1));
        if (nextOffset > newOffset) break;

        fileNames.remove(0);
        fileCount--;
      }

      if (fileCount > 0) {
        final long firstOffset = offsetFromFileName(fileNames.get(0));
        if (newOffset < firstOffset) {
          throw new IOException(getName() + " invalid offset " + newOffset + ", first log available is " + firstOffset);
        }

        // calculate offset & next offset
        this.offset = newOffset;
        this.nextOffset = (fileNames.size() > 1) ? offsetFromFileName(fileNames.get(1)) : maxOffset;
        Logger.debug("{} new offset set to {}/{}, files {}", getName(), offset, nextOffset, fileNames);
        return;
      }

      throw new UnsupportedOperationException("unexpected offset " + newOffset + " files " + fileNames);
    }

    public synchronized boolean hasMore() {
      if (getBlockAvailable() > 0) return true;
      if (fileNames.size() < 2) return false;

      // grab the next log
      nextLog();
      return getBlockAvailable() > 0;
    }

    public synchronized long getBlockOffset() {
      return blkOffset;
    }

    public synchronized long getBlockAvailable() {
      return nextOffset - offset;
    }

    public synchronized File getBlockFile() {
      // TODO: PERF: cache?
      return new File(logsDir, fileNames.get(0));
    }

    public synchronized File getLastBlockFile() {
      if (fileNames.isEmpty()) return null;

      // TODO: PERF: cache?
      final String name = fileNames.get(fileNames.size() - 1);
      return new File(logsDir, name);
    }

    public synchronized File addNewFile() {
      final String name = newFileName(maxOffset);
      this.fileNames.add(name);
      return new File(logsDir, name);
    }

    public synchronized void addData(final long length) {
      if (fileNames.size() == 1) {
        this.nextOffset += length;
      }
      this.maxOffset += length;

      for (final Consumer<LogsTracker> consumer: this.dataPublishedNotifier) {
        consumer.accept(this);
      }
    }

    public synchronized void registerDataPublishedListener(final Consumer<LogsTracker> consumer) {
      dataPublishedNotifier.add(consumer);
    }

    public synchronized void consume(final long length) {
      final long blkAvail = getBlockAvailable();

      this.offset += length;
      this.blkOffset += length;
      Logger.debug("{} consume {}. avail:{} offset:{} blkOffset:{}", getName(), length, blkAvail, offset, blkOffset);
      if (length < blkAvail) return;

      // nothing more available
      if (offset == maxOffset) {
        Logger.trace("{} log {} fully consumed. current offset {}/{}", getName(), fileNames.get(0), offset, maxOffset);
        return;
      }

      // grab the next log
      nextLog();
    }

    private void nextLog() {
      // grab the next log
      final String fileName = fileNames.remove(0);
      Logger.trace("{} log {} fully consumed. current offset {}/{}", getName(), fileName, offset, maxOffset);

      // check the next file and the current offset
      final String nextFile = fileNames.get(0);
      if (offsetFromFileName(nextFile) != offset) {
        throw new IllegalStateException(getName() + " expected log starting from offset " + offset + ", got " + nextFile);
      }

      // calculate next offset
      this.blkOffset = 0;
      this.nextOffset = (fileNames.size() > 1) ? offsetFromFileName(fileNames.get(1)) : maxOffset;
    }
  }

  public static class LogSyncMessage implements JournalEntry {
    private final String topic;
    private final byte[] message;

    public LogSyncMessage(final String topic, final String message) {
      this(topic, message.getBytes());
    }

    public LogSyncMessage(final String topic, final byte[] message) {
      this.topic = topic;
      this.message = message;
    }

    @Override
    public String getGroupId() {
      return topic;
    }

    @Override
    public void release() {
      // no-op
    }
  }

  public static class LogSyncMessageWriter implements JournalEntryWriter<LogSyncMessage> {
    @Override
    public void writeEntry(final PagedByteArray buffer, final LogSyncMessage entry) {
      buffer.addFixed32(entry.message.length);
      buffer.add(entry.message);
    }
  }

  public static void main(final String[] args) throws Exception {
    final LogsStorage storage = new LogsStorage(new File("logs.sync"));

    if (false) {
      final JournalAsyncWriter<LogSyncMessage> journal = new JournalAsyncWriter<>("pubsub", new LogSyncMessageWriter());
      journal.registerWriter(new LogWriter(groupId -> {
        final LogsTracker tracker = new LogsTracker(storage.getTopicDir(groupId));
        tracker.loadFiles();
        return tracker;
      }));

      journal.start(100);
      for (int i = 0; i < 10000; ++i) {
        journal.addToLogQueue(Thread.currentThread(), new LogSyncMessage("topic-" + (i % 5), "Hello Message " + i));
        if ((i + 1) % 1000 == 0) Thread.sleep(250);
      }
      journal.stop();
      journal.close();
    }

    if (true) {
      final LogsTracker tracker = new LogsTracker(storage.getTopicDir("service.task.bHb0DAu7ys34XpwVCnilH"));
      tracker.loadFiles();
      tracker.setOffset(0);

      for (int i = 0; tracker.hasMore(); ++i) {
        final File block = tracker.getBlockFile();
        System.out.println("OFFSET:" + tracker.getOffset()
                        + " BLKOFF: " + tracker.getBlockOffset()
                        + " AVAIL: " + tracker.getBlockAvailable()
                        + " FILE: " + tracker.getBlockFile()
                        + " index: " + i);
        SimpleLogEntryReader.read(block, tracker.getBlockOffset(), tracker.getBlockAvailable(), data -> {
          System.out.println(" -> entry: " + CborFormat.INSTANCE.fromBytes(data, JsonObject.class));
        });
        tracker.consume(tracker.getBlockAvailable());
        break;
      }
    }
  }
}
