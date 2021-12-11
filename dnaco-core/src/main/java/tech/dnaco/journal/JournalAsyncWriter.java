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

package tech.dnaco.journal;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.collections.lists.ListUtil;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.telemetry.TelemetryCollectorRegistry;
import tech.dnaco.threading.ThreadData;
import tech.dnaco.threading.ThreadData.ThreadLocalData;
import tech.dnaco.threading.ThreadUtil;
import tech.dnaco.time.TimeUtil;

public class JournalAsyncWriter<T extends JournalEntry> implements AutoCloseable {
  private final CopyOnWriteArrayList<JournalWriter<T>> writers = new CopyOnWriteArrayList<>();
  private final ThreadData<JournalBuffer<T>> localBuffers = new ThreadData<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Supplier<JournalBuffer<T>> bufferSupplier;

  private final JournalStats stats;
  private final String name;

  private LogFlusherThread flusherThread;

  private int threadBackPressureSize = 128 << 20; // 128M

  public JournalAsyncWriter(final String name, final Supplier<JournalBuffer<T>> bufferSupplier) {
    this.name = name;
    this.bufferSupplier = bufferSupplier;
    this.stats = TelemetryCollectorRegistry.INSTANCE.register(name + "_journal", name + " Journal", null, new JournalStats());
  }

  public boolean isRunning() {
    return running.get();
  }

  public void start(final int intervalMs) {
    if (!running.compareAndSet(false, true)) {
      Logger.logToStderr(LogLevel.WARNING, "{} logger already started", name);
      return;
    }

    Logger.debug("starting {} async-log writer: {}", name, writers);
    this.flusherThread = new LogFlusherThread(intervalMs);
    this.flusherThread.start();
  }

  public void stop() {
    if (!running.compareAndSet(true, false)) {
      Logger.logToStderr(LogLevel.WARNING, "{} logger already stopped", name);
      return;
    }

    Logger.debug("stopping {} log writers: {}", name, writers);
    this.flusherThread.forceFlush();
    ThreadUtil.shutdown(flusherThread);
  }

  @Override
  public void close() {
    stop();
  }

  public void registerWriter(final JournalWriter<T> writer) {
    this.writers.add(writer);
  }

  public void unregisterWriter(final JournalWriter<T> writer) {
    this.writers.remove(writer);
  }

  public void setThreadBackPressureSize(final int size) {
    this.threadBackPressureSize = size;
  }

  public void addToLogQueue(final Thread currentThread, final T entry) {
    try {
      final int bufSize;
      try (ThreadLocalData<JournalBuffer<T>> buffer = localBuffers.computeIfAbsent(currentThread, bufferSupplier::get)) {
        bufSize = buffer.get().add(entry);
        entry.release();
      }

      if (bufSize >= threadBackPressureSize) {
        flusherThread.forceFlush();
        flusherThread.waitForFlush();
      }
    } catch (final Throwable e) {
      Logger.logToStderr(LogLevel.ERROR, e, "unable to add entry to the journal: thread={} entry={}", currentThread, entry);
    }
  }

  private final class LogFlusherThread extends Thread {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition flushIntervalCond = lock.newCondition();
    private final int intervalMs;

    private boolean flushing = false;

    private LogFlusherThread(final int intervalMs) {
      super(name + "Flusher");
      this.intervalMs = intervalMs;
    }

    public void waitForFlush() {
      lock.lock();
      try {
        System.out.println("WAIT FOR FLUSH: " + flushing);
      } finally {
        lock.unlock();
      }
    }

    private void forceFlush() {
      if (lock.tryLock()) {
        try {
          flushIntervalCond.signalAll();
        } finally {
          lock.unlock();
        }
      }
    }

    @Override
    public void run() {
      lock.lock();
      try {
        long cleanerNs = 0;
        while (isRunning()) {
          final long startTimeNs = System.nanoTime();
          if ((startTimeNs - cleanerNs) > TimeUnit.HOURS.toNanos(1)) {
            // TODO: execute in another thread
            for (final JournalWriter<T> writer: writers) writer.manageOldLogs();
            cleanerNs = System.nanoTime();
            stats.addManageOldLogs(cleanerNs - startTimeNs);
          }

          // wait the specified interval before the next flush
          ThreadUtil.conditionAwait(flushIntervalCond, intervalMs, TimeUnit.MILLISECONDS);

          flushQueue();
        }

        // force a last flush
        flushQueue();
      } catch (final Throwable e) {
        Logger.logToStderr(LogLevel.ALERT, e, "LogWriter shutting down");
      } finally {
        lock.unlock();
      }
    }

    private void flushQueue() {
      // take charge of the local thread buffers
      final List<JournalBuffer<T>> buffers = localBuffers.getThreadData();
      if (ListUtil.isEmpty(buffers)) return;

      final long now = TimeUtil.currentUtcMillis();
      final long startNs = System.nanoTime();
      final HashSet<String> groupIds = new HashSet<>(64);
      long bufSize = 0;
      this.flushing = true;
      try {
        for (final JournalBuffer<T> buf: buffers) {
          buf.flush();

          groupIds.addAll(buf.getGroupIds());
          bufSize += buf.size();
        }

        for (final String groupId: groupIds) {
          for (final JournalWriter<T> writer: writers) {
            writer.writeBuffers(groupId, buffers);
          }
        }
      } finally {
        this.flushing = false;
        final long elapsedNs = System.nanoTime() - startNs;
        stats.addFlush(now, groupIds.size(), bufSize, elapsedNs);
      }
    }
  }

  public interface JournalEntryWriter<T extends JournalEntry> {
    void writeEntry(PagedByteArray buffer, T entry);
  }
}
