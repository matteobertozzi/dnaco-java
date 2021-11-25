package tech.dnaco.net.logsync;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.time.ZoneOffset;
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
    return String.format("%020d.%s", offset, HumansUtil.toHumanTs(ZonedDateTime.now(ZoneOffset.UTC)));
  }

  public static long offsetFromFileName(final String name) {
    final int offsetEof = name.indexOf('.');
    return Long.parseLong(name, 0, offsetEof, 10);
  }

  public static long timestampFromFileName(final String name) {
    final int offsetEof = name.indexOf('.');
    final long humanTs = Long.parseLong(name, offsetEof + 1, name.length(), 10);
    return HumansUtil.fromUtcHumanTs(humanTs).toInstant().toEpochMilli();
  }

  @FunctionalInterface
  public interface LogsTrackerSupplier {
    LogsFileTracker getLogsTracker(String groupId);
  }

  @FunctionalInterface
  public interface LogsTrackerConsumer {
    void accept(LogsFileTracker tracker);
  }

  @FunctionalInterface
  public interface LogOffsetStore {
    void store(String logsId, long offset) throws Exception;
  }

  public static class LogsTrackerManager {
    private final CopyOnWriteArrayList<LogsTrackerConsumer> newLogsListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, LogsFileTracker> trackers = new ConcurrentHashMap<>();
    private final LogsStorage storage;

    public LogsTrackerManager(final LogsStorage storage) {
      this.storage = storage;
    }

    public Set<String> getLogsIds() {
      return storage.getLogsIds();
    }

    public LogsTrackerManager registerNewLogsListener(final LogsTrackerConsumer consumer) {
      this.newLogsListeners.add(consumer);
      return this;
    }

    public LogsFileTracker get(final String logsId) {
      LogsFileTracker tracker = trackers.get(logsId);
      if (tracker != null) return tracker;

      synchronized (this) {
        tracker = trackers.get(logsId);
        if (tracker != null) return tracker;

        tracker = newTracker(logsId);
        trackers.put(logsId, tracker);
      }

      for (final LogsTrackerConsumer listener: newLogsListeners) {
        listener.accept(tracker);
      }
      return tracker;
    }

    public void remove(final String logId) {
      trackers.remove(logId);
    }

    private LogsFileTracker newTracker(final String logsId) {
      final LogsFileTracker tracker = new LogsFileTracker(storage.getLogsDir(logsId));
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
      final LogsFileTracker logsTracker = logsTrackerSupplier.getLogsTracker(groupId);

      File logFile = logsTracker.getLastBlockFile();
      if (logFile == null || logFile.length() > ROLL_SIZE) {
        Logger.debug("roll logFile:{} logSize:{}", logFile, logFile != null ? logFile.length() : 0);
        logFile = logsTracker.addNewFile();
      }

      logFile.getParentFile().mkdirs();
      long flushSize = 0;
      final long startTimeNs = System.nanoTime();
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
        Logger.debug("flush data: {} {} {}", logFile, HumansUtil.humanSize(flushSize), HumansUtil.humanTimeSince(startTimeNs));
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
    public static long read(final File logFile, final long offset, final long avail,
        final LogEntryProcessor processor) throws Exception {
      try (final LimitedInputStream stream = new LimitedInputStream(new FileInputStream(logFile), offset + avail)) {
        stream.skipNBytes(offset);

        long consumed = 0;
        while (stream.available() > 0) {
          final byte[] block;
          try {
            block = readBlock(stream);
          } catch (final EOFException e) {
            Logger.trace("got EOF while reading block {}", e.getMessage());
            break;
          }

          consumed = stream.consumed();
          try (ByteArrayReader blockReader = new ByteArrayReader(block)) {
            while (blockReader.available() > 0) {
              final int length = IntDecoder.readUnsignedVarInt(blockReader);
              final ByteArraySlice data = new ByteArraySlice(block, blockReader.readOffset(), length);
              processor.process(data);
              blockReader.skip(length);
            }
          }
        }
        return consumed;
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

    public Set<String> getLogsIds() {
      final String[] logsIds = logsDir.list();
      return logsIds != null ? Set.of(logsIds) : Set.of();
    }

    public File getLogsDir(final String logsId) {
      return new File(logsDir, logsId);
    }
  }

  public static class LogsConsumer {
    private final ArrayList<String> fileNames = new ArrayList<>();
    private final LogsFileTracker tracker;
    private final String name;

    private long offset;
    private long blkOffset;
    private long nextOffset;
    private long maxOffset;

    public LogsConsumer(final LogsFileTracker tracker, final String name, final long offset) {
      this.tracker = tracker;
      this.name = name;
      this.setOffset(offset);
    }

    public String getName() {
      return name;
    }

    public String getLogsId() {
      return tracker.getLogsId();
    }

    public synchronized void setOffset(final long newOffset) {
      this.offset = 0;
      this.blkOffset = 0;
      this.nextOffset = 0;
      this.maxOffset = tracker.getMaxOffset();

      fileNames.clear();
      fileNames.addAll(tracker.getFileNames());
      if (fileNames.isEmpty()) return;

      if (newOffset > maxOffset) {
        throw new IllegalArgumentException(getLogsId() + " invalid offset " + newOffset + ", max offset is " + maxOffset);
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
          throw new IllegalArgumentException(getLogsId() + " invalid offset " + newOffset + ", first log available is " + firstOffset);
        }

        // calculate offset & next offset
        this.offset = newOffset;
        this.blkOffset = newOffset - firstOffset;
        this.nextOffset = (fileNames.size() > 1) ? offsetFromFileName(fileNames.get(1)) : maxOffset;
        Logger.info("{} new offset set to {}/{} {blkOffset}, files {}", getLogsId(), offset, nextOffset, blkOffset, fileNames);
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

    public synchronized long getMaxOffset() {
      return maxOffset;
    }

    public synchronized long getOffset() {
      return offset;
    }

    public synchronized long getBlockOffset() {
      return blkOffset;
    }

    public synchronized long getBlockAvailable() {
      return nextOffset - offset;
    }

    public synchronized File getBlockFile() {
      // TODO: PERF: cache?
      return tracker.getFile(fileNames.get(0));
    }

    public synchronized void consume(final long length) {
      if (length < 0 || length > getBlockAvailable()) {
        Logger.fatal("CONSUMED SOMETHING WRONG {}: {consumedLength} {blkAvail}",
          getLogsId(), length, getBlockAvailable());
      }

      this.offset += length;
      this.blkOffset += length;

      final long blkAvail = getBlockAvailable();
      Logger.debug("{} consume {}. avail:{} offset:{} blkOffset:{}", getLogsId(), length, blkAvail, offset, blkOffset);
      if (blkAvail > 0) return;

      // nothing more available
      if (offset == maxOffset) {
        Logger.trace("{} log {} fully consumed. current offset {}/{}", getLogsId(), fileNames.get(0), offset, maxOffset);
        return;
      }

      if (fileNames.size() == 1) {
        Logger.fatal("TRYING TO GRAB A NON EXISTENT NEXT LOG {}: {offset} {maxOffset}: {blkAvail} {blkOff}: {nextOffset} {files}",
          getLogsId(), offset, maxOffset, blkAvail, blkOffset, nextOffset, fileNames);
      }

      // grab the next log
      nextLog();
    }

    private void nextLog() {
      // grab the next log
      final String fileName = fileNames.remove(0);
      Logger.trace("{} log {} fully consumed. current offset {}/{}", getLogsId(), fileName, offset, maxOffset);

      // check the next file and the current offset
      final String nextFile = fileNames.get(0);
      if (offsetFromFileName(nextFile) != offset) {
        throw new IllegalStateException(getLogsId() + " expected log starting from offset " + offset + ", got " + nextFile);
      }

      // calculate next offset
      this.blkOffset = 0;
      this.nextOffset = (fileNames.size() > 1) ? offsetFromFileName(fileNames.get(1)) : maxOffset;
      Logger.info("{} {currentFile} {offset} {nextOffset}", getLogsId(), nextFile, offset, nextOffset);
    }

    // ==========================================================================================
    //  Publisher methods
    // ==========================================================================================
    private synchronized void addFile(final String name) {
      this.fileNames.add(name);
    }

    private synchronized void addData(final long length) {
      if (fileNames.size() == 1) {
        // the data is being appended to the current file
        this.nextOffset += length;
      }
      this.maxOffset += length;
    }

    public void registerDataPublishedListener(final Consumer<LogsFileTracker> consumer) {
      tracker.registerDataPublishedListener(consumer);
    }
  }

  public static class LogsFileTracker {
    private final ArrayList<Consumer<LogsFileTracker>> dataPublishedNotifier = new ArrayList<>();
    private final ArrayList<LogsConsumer> consumers = new ArrayList<>();
    private final ArrayList<String> fileNames = new ArrayList<>();
    private final File logsDir;

    private long maxOffset = 0;
    private long lastPublished = 0;

    public LogsFileTracker(final File logsDir) {
      this.logsDir = logsDir;
      this.maxOffset = 0;
      this.lastPublished = 0;
    }

    public String getLogsId() {
      return logsDir.getName();
    }

    public File getFile(final String name) {
      return new File(logsDir, name);
    }

    public synchronized File getLastBlockFile() {
      if (fileNames.isEmpty()) return null;

      // TODO: PERF: cache?
      return getFile(fileNames.get(fileNames.size() - 1));
    }

    private synchronized List<String> getFileNames() {
      return fileNames;
    }

    public synchronized void loadFiles() {
      final String[] files = logsDir.list();
      if (ArrayUtil.isEmpty(files)) {
        Logger.trace("no log file available for dir: {}", logsDir);
        this.fileNames.clear();
        this.maxOffset = 0;
        return;
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
    }

    private long getGatingSequence() {
      long minSeq = maxOffset;
      for (final LogsConsumer consumer: this.consumers) {
        minSeq = Math.min(minSeq, consumer.getOffset());
      }
      return minSeq;
    }

    private void cleanupFiles() {
      cleanupFiles(Duration.ofHours(12));
    }

    public synchronized boolean cleanupFiles(final Duration retainTime) {
      if (fileNames.isEmpty()) {
        Logger.trace("no log files: {}", logsDir);
        logsDir.delete();
        return true;
      }

      final long gatingSequence = getGatingSequence();

      final long retainMs = retainTime.toMillis();
      final long now = System.currentTimeMillis();
      while (!fileNames.isEmpty()) {
        final String fileName = fileNames.get(0);
        final long logOffset = offsetFromFileName(fileName);
        if (gatingSequence < logOffset) break;

        Logger.info("{} log file {} is candidate for removal", getLogsId(), fileName);
        final long delta = now - timestampFromFileName(fileName);
        if (delta < retainMs) {
          Logger.debug("{} log file {} is in the retain period {}: {}",
            getLogsId(), fileName, HumansUtil.humanTimeMillis(retainMs), HumansUtil.humanTimeMillis(delta));
          break;
        }

        Logger.info("{} removing {} created {}", getLogsId(), fileName, HumansUtil.humanTimeMillis(delta));
        new File(logsDir, fileName).delete();
        fileNames.remove(0);
      }

      if (fileNames.isEmpty()) {
        Logger.info("{} removing empty dir", getLogsId(), logsDir);
        logsDir.delete();
        return true;
      }
      return false;
    }

    // ==========================================================================================
    //  Publisher
    // ==========================================================================================
    public synchronized long getMaxOffset() {
      return maxOffset;
    }

    public synchronized File addNewFile() {
      final String name = newFileName(maxOffset);
      this.fileNames.add(name);
      for (final LogsConsumer consumer: consumers) {
        consumer.addFile(name);
      }
      return new File(logsDir, name);
    }

    public synchronized void addData(final long length) {
      this.lastPublished = System.currentTimeMillis();
      this.maxOffset += length;
      for (final LogsConsumer consumer: consumers) {
        consumer.addData(length);
      }

      for (final Consumer<LogsFileTracker> consumer: this.dataPublishedNotifier) {
        consumer.accept(this);
      }
    }

    public synchronized void registerDataPublishedListener(final Consumer<LogsFileTracker> consumer) {
      dataPublishedNotifier.add(consumer);
    }

    // ==========================================================================================
    //  Consumer
    // ==========================================================================================
    public synchronized LogsConsumer newConsumer(final String name, final long offset) {
      final LogsConsumer consumer = new LogsConsumer(this, name, offset);
      Logger.debug("{} new consumer {} offset {reqOffset}/{offset}", getLogsId(), name, offset, consumer.getOffset());
      consumers.add(consumer);
      return consumer;
    }
  }

  public static class LogSyncMessage implements JournalEntry {
    private final String logId;
    private final byte[] message;

    public LogSyncMessage(final String logId, final String message) {
      this(logId, message.getBytes());
    }

    public LogSyncMessage(final String logId, final byte[] message) {
      this.logId = logId;
      this.message = message;
    }

    @Override
    public String getGroupId() {
      return logId;
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

    if (true) {
      final long now = System.currentTimeMillis();
      final long t = timestampFromFileName("00000000000000000000.20211115205345");
      System.out.println("NOW: " + now);
      System.out.println("TIM: " + t);
      System.out.println("DEL: " + (now - t));
      return;
    }

    if (false) {
      final JournalAsyncWriter<LogSyncMessage> journal = new JournalAsyncWriter<>("pubsub", new LogSyncMessageWriter());
      journal.registerWriter(new LogWriter(groupId -> {
        final LogsFileTracker tracker = new LogsFileTracker(storage.getLogsDir(groupId));
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
      final LogsFileTracker tracker = new LogsFileTracker(storage.getLogsDir("service.task.bHb0DAu7ys34XpwVCnilH"));
      tracker.loadFiles();

      final LogsConsumer consumer = tracker.newConsumer("foo", 0);
      consumer.setOffset(0);

      for (int i = 0; consumer.hasMore(); ++i) {
        final File block = consumer.getBlockFile();
        System.out.println("OFFSET:" + consumer.getOffset()
                        + " BLKOFF: " + consumer.getBlockOffset()
                        + " AVAIL: " + consumer.getBlockAvailable()
                        + " FILE: " + consumer.getBlockFile()
                        + " index: " + i);
        final long consumed = SimpleLogEntryReader.read(block, consumer.getBlockOffset(), consumer.getBlockAvailable(), data -> {
          System.out.println(" -> entry: " + CborFormat.INSTANCE.fromBytes(data, JsonObject.class));
        });
        consumer.consume(consumed);
        break;
      }
    }
  }
}
