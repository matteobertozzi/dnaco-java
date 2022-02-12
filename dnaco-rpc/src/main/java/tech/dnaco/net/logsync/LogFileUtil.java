/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import tech.dnaco.bytes.ByteArrayReader;
import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.collections.LongValue;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.collections.arrays.paged.PagedByteArrayWriter;
import tech.dnaco.data.compression.ZstdUtil;
import tech.dnaco.io.IOUtil;
import tech.dnaco.io.LimitedInputStream;
import tech.dnaco.io.MultiFileInputStream;
import tech.dnaco.io.MultiFileInputStream.InputStreamSupplier;
import tech.dnaco.journal.JournalAsyncWriter;
import tech.dnaco.journal.JournalAsyncWriter.JournalEntryWriter;
import tech.dnaco.journal.JournalBuffer;
import tech.dnaco.journal.JournalEntry;
import tech.dnaco.journal.JournalGroupedBuffer;
import tech.dnaco.journal.JournalWriter;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.measure.TelemetryMeasureBuffer;

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

  public interface LogsEventListener {
    void newLogEvent(LogsFileTracker tracker);
    void removeLogEvent(LogsFileTracker tracker);
  }

  public static class LogWriter implements JournalWriter<LogSyncMessage> {
    private static final long ROLL_SIZE = 32 << 10;

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
    public static long read(final LogsConsumer consumer, final long timeLimitNs, final LogEntryProcessor processor) throws Exception {
      final LogsConsumerInputStreamSupplier streamSupplier = new LogsConsumerInputStreamSupplier(consumer);
      try (final MultiFileInputStream stream = new MultiFileInputStream(streamSupplier)) {
        return read(stream, streamSupplier, timeLimitNs, processor);
      }
    }

    private static long read(final MultiFileInputStream stream,
        final LogsConsumerInputStreamSupplier streamSupplier, final long timeLimitNs,
        final LogEntryProcessor processor) throws Exception {
      final LongValue blkLength = new LongValue();
      final long startTime = System.nanoTime();
      long consumed = 0;
      while (stream.available() > 0) {
        final byte[] block;
        try {
          block = readBlock(stream, blkLength);
          consumed += blkLength.get();
        } catch (final EOFException e) {
          System.out.println("EOF");
          Logger.trace("got EOF while reading block {consumed}: {}", consumed, e.getMessage());
          break;
        }

        try (ByteArrayReader blockReader = new ByteArrayReader(block)) {
          while (blockReader.available() > 0) {
            final int length = IntDecoder.readUnsignedVarInt(blockReader);
            final ByteArraySlice data = new ByteArraySlice(block, blockReader.readOffset(), length);
            processor.process(data);
            blockReader.skipNBytes(length);
          }
        }

        if (streamSupplier.getBlkIndex() > 0 || (System.nanoTime() - startTime) >= timeLimitNs) {
          break;
        }
      }
      return consumed;
    }

    private static byte[] readBlock(final InputStream stream, final LongValue consumed) throws IOException {
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
      consumed.set(1 + blkLenBytes + zblkLenBytes + zLength);
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

  private static final class LogsConsumerInputStreamSupplier implements InputStreamSupplier {
    private final ArrayList<String> fileNames;
    private final LogsConsumer consumer;
    private long offset;
    private long blkIndex;

    private LogsConsumerInputStreamSupplier(final LogsConsumer consumer) {
      this.fileNames = consumer.getFileNames();
      this.consumer = consumer;
      this.offset = consumer.getOffset();
      this.blkIndex = 0;
    }

    private long getBlkIndex() {
      return blkIndex;
    }

    @Override
    public InputStream get() throws IOException {
      if (fileNames.isEmpty()) return null;

      final boolean isNotFirst = (offset > consumer.getOffset());

      // grab the next log
      final String fileName = fileNames.remove(0);
      final File file = consumer.getFile(fileName);
      final long fileLength = file.length();
      final long avail = Math.min(fileLength, consumer.getMaxOffset() - offset);
      offset += avail;
      blkIndex++;

      if (isNotFirst) {
        return new LimitedInputStream(new FileInputStream(file), avail);
      }

      final LimitedInputStream stream = new LimitedInputStream(new FileInputStream(file), avail);
      stream.skipNBytes(consumer.getBlockOffset());
      return stream;
    }
  }

  public static class LogSyncMessage implements JournalEntry {
    private final String logId;
    private final byte[] message;
    private final int offset;
    private final int length;

    public LogSyncMessage(final String logId, final String message) {
      this(logId, message.getBytes());
    }

    public LogSyncMessage(final String logId, final byte[] message) {
      this(logId, message, 0, message.length);
    }

    public LogSyncMessage(final String logId, final byte[] buf, final int off, final int len) {
      this.logId = logId;
      this.message = buf;
      this.offset = off;
      this.length = len;
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
    public static final LogSyncMessageWriter INSTANCE = new LogSyncMessageWriter();

    private LogSyncMessageWriter() {
      // no-op
    }

    @Override
    public void writeEntry(final PagedByteArray buffer, final LogSyncMessage entry) {
      buffer.addFixed32(entry.length);
      buffer.add(entry.message, entry.offset, entry.length);
    }
  }

  public static final Supplier<JournalBuffer<LogSyncMessage>> LOG_SYNC_MESSAGE_JOURNAL_SUPPLIER =
    () -> new JournalGroupedBuffer<>(LogSyncMessageWriter.INSTANCE);

  public static void main(final String[] args) throws Exception {
    final LogsStorage storage = new LogsStorage(new File("logs.sync"));

    if (true) {
      final LogsFileTracker tracker = new LogsFileTracker(storage.getLogsDir("bang"));
      tracker.loadFiles();

      final LogsConsumer consumer = tracker.newConsumer("foo", 0);
      consumer.setOffset(0);

      final long consumed = SimpleLogEntryReader.read(consumer, Duration.ofMinutes(1).toNanos(), data -> {
        System.out.println(BytesUtil.toHexString(data.buffer()));
        TelemetryMeasureBuffer.read(data, (tenantId, measure) -> {
          Logger.debug("processing {tenantId} {measure}", tenantId, measure);
        });
      });
      return;
    }

    if (false) {
      new LogSyncMessageWriter();
      final JournalAsyncWriter<LogSyncMessage> journal = new JournalAsyncWriter<>("pubsub", LOG_SYNC_MESSAGE_JOURNAL_SUPPLIER);
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
      final LogsFileTracker tracker = new LogsFileTracker(storage.getLogsDir("service.logs.usXoYD4NOY4Iial1Aw5yq"));
      tracker.loadFiles();

      final LogsConsumer consumer = tracker.newConsumer("foo", 0);
      consumer.setOffset(0);

      for (int i = 0; consumer.hasMore(); ++i) {
        System.out.println(" OFFSET:" + consumer.getOffset()
                        + " BLKOFF: " + consumer.getBlockOffset()
                        + " AVAIL: " + consumer.getBlockAvailable()
                        + " FILE: " + consumer.getBlockFile()
                        + " index: " + i);
        //final long consumed = SimpleLogEntryReader.read(block, consumer.getBlockOffset(), consumer.getBlockAvailable(), data -> {
          //System.out.println(" -> entry: " + CborFormat.INSTANCE.fromBytes(data, JsonObject.class));
        //});
        final long consumed = SimpleLogEntryReader.read(consumer, Duration.ofMinutes(1).toNanos(), data -> {});
        consumer.consume(consumed);
      }
    }
  }
}
