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

package tech.dnaco.threading;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import tech.dnaco.collections.lists.ListUtil;

public final class ThreadUtil {
  private ThreadUtil() {
    // no-op
  }

  // ================================================================================
  // Sleep related
  // ================================================================================
  public static boolean sleep(final long millis) {
    return sleep(millis, TimeUnit.MILLISECONDS);
  }

  public static boolean sleep(final long duration, final TimeUnit unit) {
    try {
      Thread.sleep(unit.toMillis(duration));
      return true;
    } catch (final InterruptedException e) {
      Thread.interrupted();
      return false;
    }
  }

  public static boolean sleepWithoutInterrupt(final long msToWait) {
    long timeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    final long endTime = timeMillis + msToWait;
    boolean interrupted = false;
    while (timeMillis < endTime) {
      try {
        Thread.sleep(endTime - timeMillis);
      } catch (final InterruptedException ex) {
        interrupted = true;
      }
      timeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return interrupted;
  }

  // ================================================================================
  // Sleep related
  // ================================================================================
  public static boolean sleep(final AtomicBoolean running, final long wakeInterval, final long millis) {
    final long expireTs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    while (running.get()) {
      final long delta = TimeUnit.NANOSECONDS.toMillis(expireTs - System.nanoTime());
      if (delta <= 0) break;

      if (!ThreadUtil.sleep(Math.min(delta, wakeInterval))) {
        return false;
      }
    }
    return true;
  }

  // ================================================================================
  // Thread related
  // ================================================================================
  public static void shutdown(final Thread t) {
    shutdown(t, Long.MAX_VALUE);
  }

  public static void shutdown(final Thread thread, final long joinWait, final TimeUnit unit) {
    shutdown(thread, unit.toMillis(joinWait));
  }

  public static void shutdown(final Thread thread, final long joinWait) {
    if (thread == null) return;

    boolean interrupted = false;
    while (thread.isAlive()) {
      try {
        thread.join(joinWait);
      } catch (final InterruptedException e) {
        System.err.println(thread.getName() + "; joinWait=" + joinWait);
        interrupted = true;
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  public static void shutdown(final Collection<Thread> threads) {
    if (ListUtil.isEmpty(threads)) return;
    for (final Thread thread: threads) {
      shutdown(thread, Long.MAX_VALUE);
    }
  }

  public static void shutdown(final Collection<Thread> threads, final long joinWait, final TimeUnit unit) {
    if (ListUtil.isEmpty(threads)) return;
    for (final Thread thread: threads) {
      shutdown(thread, unit.toMillis(joinWait));
    }
  }

  public static boolean conditionAwait(final Condition waitCond) {
    try {
      waitCond.await();
      return true;
    } catch (final InterruptedException e) {
      Thread.interrupted();
      System.err.println("wait-cond got interrupted");
      return false;
    }
  }

  public static boolean conditionAwait(final Condition waitCond, final Lock lock) {
    if (lock.tryLock()) {
      try {
        return conditionAwait(waitCond);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  public static boolean conditionAwait(final Condition waitCond, final long time, final TimeUnit unit) {
    try {
      return waitCond.await(time, unit);
    } catch (final InterruptedException e) {
      Thread.interrupted();
      System.err.println("wait-cond got interrupted");
      return false;
    }
  }

  public static boolean conditionAwait(final Lock lock, final Condition waitCond, final long time, final TimeUnit unit) {
    lock.lock();
    try {
      return waitCond.await(time, unit);
    } catch (final InterruptedException e) {
      Thread.interrupted();
      return false;
    } finally {
      lock.unlock();
    }
  }

  public static boolean conditionTrySignal(final Condition waitCond, final Lock lock) {
    if (lock.tryLock()) {
      try {
        waitCond.signal();
      } finally {
        lock.unlock();
      }
      return true;
    }
    return false;
  }

  public static boolean conditionTrySignalAll(final Condition waitCond, final Lock lock) {
    if (lock.tryLock()) {
      try {
        waitCond.signalAll();
      } finally {
        lock.unlock();
      }
      return true;
    }
    return false;
  }

  public static void runInThreads(final String threadName, final int count, final Runnable runnable) {
    runInThreads(new NamedThreadFactory(threadName), count, runnable);
  }

  public static void runInThreads(final ThreadFactory threadFactory, final int count, final Runnable runnable) {
    final List<Thread> threads = runInThreadsNoWait(threadFactory, count, runnable);
    shutdown(threads);
  }

  public static Thread runInThreadNoWait(final String threadName, final Runnable runnable) {
    final Thread thread = new Thread(runnable, threadName);
    thread.start();
    return thread;
  }

  public static List<Thread> runInThreadsNoWait(final String threadName, final int count, final Runnable runnable) {
    return runInThreadsNoWait(new NamedThreadFactory(threadName), count, runnable);
  }

  public static List<Thread> runInThreadsNoWait(final ThreadFactory threadFactory, final int count, final Runnable runnable) {
    final Thread[] threads = new Thread[count];
    for (int i = 0; i < count; ++i) threads[i] = threadFactory.newThread(runnable);
    for (int i = 0; i < count; ++i) threads[i].start();
    return List.of(threads);
  }
}
