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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class ThreadData<T> {
  private final ConcurrentHashMap<Thread, ThreadLocalData<T>> threadData = new ConcurrentHashMap<>();

  public static void main(final String[] args) throws Exception {
    final ThreadData<String> localBuffers = new ThreadData<>();
    BenchUtil.run("foo", 1_000_000, () -> localBuffers.computeIfAbsent(Thread.currentThread(), () -> "hello").get());
  }

  public ThreadLocalData<T> get() {
    return get(Thread.currentThread());
  }

  public ThreadLocalData<T> get(final Thread thread) {
    while (true) {
      final ThreadLocalData<T> data = threadData.get(thread);
      if (data == null) break;
      if (data.tryAcquire()) return data;
    }
    return null;
  }

  public ThreadLocalData<T> computeIfAbsent(final Supplier<T> supplier) {
    return computeIfAbsent(Thread.currentThread(), supplier);
  }

  public ThreadLocalData<T> computeIfAbsent(final Thread thread, final Supplier<T> supplier) {
    while (true) {
      ThreadLocalData<T> data = threadData.get(thread);
      if (data == null) {
        data = new ThreadLocalData<>(supplier.get());
        if (threadData.putIfAbsent(thread, data) != null) {
          data.acquire();
          return data;
        }
      } else if (data.tryAcquire()) {
        return data;
      }
    }
  }

  public List<T> getThreadData() {
    final ArrayList<T> threadLocalData = new ArrayList<>(threadData.size());
    for (final Thread thread: new ArrayList<>(threadData.keySet())) {
      final ThreadLocalData<T> data = threadData.remove(thread);
      data.acquire();
      threadLocalData.add(data.get());
    }
    return threadLocalData;
  }

  public static final class ThreadLocalData<T> implements AutoCloseable {
    private final ReentrantLock lock = new ReentrantLock(false);
    private T value;

    private ThreadLocalData(final T value) {
      this.value = value;
    }

    public T set(final T value) {
      final T oldValue = this.value;
      this.value = value;
      return oldValue;
    }

    public T get() {
      return value;
    }

    public boolean tryAcquire() {
      return lock.tryLock();
    }

    public void acquire() {
      lock.lock();
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }
}
