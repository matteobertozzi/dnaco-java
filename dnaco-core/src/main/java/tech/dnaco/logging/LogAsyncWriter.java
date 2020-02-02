/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.logging;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.paged.PagedByteArray;
import tech.dnaco.io.BytesInputStream;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.MaxAndAvgTimeRangeGauge;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.util.ThreadData;
import tech.dnaco.util.ThreadData.ThreadLocalData;
import tech.dnaco.util.ThreadUtil;

public class LogAsyncWriter implements AutoCloseable {
  private static final int FORCE_FLUSH_BUFFER_SIZE = (16 << 20);

  private final ThreadData<LogBuffer> logBuffers = new ThreadData<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Supplier<LogBuffer> logBufferSupplier;
  private final LogWriterTask writerTask;
  private final LogWriter writer;

  private Thread writerThread;

  public LogAsyncWriter(final LogWriter writer, final boolean binaryFormat) {
    this.writer = writer;
    this.writerTask = new LogWriterTask(2500);
    this.logBufferSupplier = () -> new LogThreadBuffer(binaryFormat);
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      Logger.logToStderr(LogLevel.WARNING, "logger already started");
    }

    System.out.println("start log writer");
    writerThread = new Thread(writerTask, getClass().getName());
    writerThread.start();
  }

  public void stop() {
    if (!running.compareAndSet(true, false)) {
      Logger.logToStderr(LogLevel.WARNING, "logger already stopped");
    }

    System.out.println("stop log writer");
    writerTask.forceFlush();
    ThreadUtil.shutdown(writerThread);
  }

  @Override
  public void close() {
    stop();
  }

  public void addToLogQueue(final Thread thread, final String projectId, final LogEntry entry) {
    try (ThreadLocalData<LogBuffer> buffer = logBuffers.computeIfAbsent(thread, logBufferSupplier)) {
      if (buffer.get().add(projectId, entry) >= FORCE_FLUSH_BUFFER_SIZE) {
        writerTask.forceFlush();
      }
    }
  }

  private static final class LogThreadBuffer implements LogBuffer {
    private final HashMap<String, LogGroupBuffer> buffers = new HashMap<>();
    private final PagedByteArray blob = new PagedByteArray(1 << 10);
    private final boolean binaryFormat;

    private LogThreadBuffer(final boolean binaryFormat) {
      this.binaryFormat = binaryFormat;
    }

    @Override
    public int size() {
      return blob.size();
    }

    @Override
    public Set<String> getProjectIds() {
      return buffers.keySet();
    }

    @Override
    public int add(final String projectId, final LogEntry entry) {
      final LogGroupBuffer logBuffer = getLogBuffer(projectId);
      logBuffer.add(blob, entry, binaryFormat);
      return blob.size();
    }

    @Override
    public long writeTo(final String projectId, final OutputStream stream) throws IOException {
      final LogGroupBuffer logBuffer = buffers.get(projectId);
      if (logBuffer == null) return 0;

      try (BytesInputStream reader = blob.getInputStream()) {
        return logBuffer.writeTo(reader, binaryFormat, stream);
      }
    }

    private LogGroupBuffer getLogBuffer(final String groupId) {
      LogGroupBuffer buffer = buffers.get(groupId);
      if (buffer != null) return buffer;

      buffer = new LogGroupBuffer();
      buffers.put(groupId, buffer);
      return buffer;
    }
  }

  private static final class LogGroupBuffer {
    private static final int ENTRY_OFFSET_EOF = 0xffffffff;

    final byte[] buffer = new byte[4];
    private int head = -1;
    private int tail = -1;

    public void add(final PagedByteArray blob, final LogEntry entry, final boolean binaryFormat) {
      final int offset = blob.size();
      IntEncoder.BIG_ENDIAN.writeFixed32(buffer, 0, ENTRY_OFFSET_EOF);
      blob.add(buffer, 0, 4);
      entry.writeBlockTo(blob, binaryFormat);

      if (tail < 0) {
        head = offset;
      } else {
        IntEncoder.BIG_ENDIAN.writeFixed(buffer, 0, offset, 4);
        blob.set(tail, buffer, 0, 4);
      }
      tail = offset;
    }

    public long writeTo(final BytesInputStream reader, final boolean binaryFormat,
        final OutputStream stream) throws IOException {
      long written = 0;
      int lastOffset = head;
      int offset = head;
      while (offset != ENTRY_OFFSET_EOF) {
        lastOffset = offset;
        reader.seekTo(offset);
        offset = IntDecoder.BIG_ENDIAN.readFixed32(reader);
        written += LogEntry.copyTo(reader, binaryFormat, stream);
      }
      // TODO: check (offset == tail)
      if (lastOffset != tail) {
        System.err.println("unexpected tail " + tail + "/" + offset);
      }
      return written;
    }
  }

  private final class LogWriterTask implements Runnable {
    private final MaxAndAvgTimeRangeGauge logBufferUsage = new TelemetryCollector.Builder()
        .setUnit(HumansUtil.HUMAN_COUNT)
        .setName("logger.logger_buffer_usage")
        .setLabel("Logger Buffer Usage")
        .register(new MaxAndAvgTimeRangeGauge(24 * 60, 5, TimeUnit.MINUTES));

    private final Lock lock = new ReentrantLock();
    private final Condition waitCond = lock.newCondition();
    private final int intervalMs;

    public LogWriterTask(final int intervalMs) {
      this.intervalMs = intervalMs;
    }

    public void forceFlush() {
      if (lock.tryLock()) {
        try {
          waitCond.signalAll();
        } finally {
          lock.unlock();
        }
      }
    }

    @Override
    public void run() {
      lock.lock();
      try {
        long cleanerTs = 0;
        while (running.get()) {
          if ((System.nanoTime() - cleanerTs) > TimeUnit.HOURS.toNanos(1)) {
            writer.manageOldLogs();
            cleanerTs = System.nanoTime();
          }

          ThreadUtil.conditionAwait(waitCond, intervalMs, TimeUnit.MILLISECONDS);

          flushQueue();
        }
        flushQueue();
      } catch (final Throwable e) {
        Logger.logToStderr(LogLevel.ALERT, e, "LogWriter shutting down");
      } finally {
        lock.unlock();
      }
    }

    private void flushQueue() {
      final List<LogBuffer> buffers = logBuffers.getThreadData();
      if (buffers.isEmpty()) return;

      long bufSize = 0;
      final HashSet<String> projectIds = new HashSet<>();
      for (final LogBuffer buf: buffers) {
        projectIds.addAll(buf.getProjectIds());
        bufSize += buf.size();
      }
      logBufferUsage.set(System.currentTimeMillis(), bufSize);

      Logger.logToStdout(LogLevel.TRACE, "flush queues: {} buffers: {}", projectIds, buffers.size());
      for (final String projectId: projectIds) {
        try {
          writer.writeQueue(projectId, buffers);
        } catch (final Throwable e) {
          Logger.logToStderr(LogLevel.ALERT, e, "unable to flush {} buffers", projectId);
        }
      }
    }
  }
}
