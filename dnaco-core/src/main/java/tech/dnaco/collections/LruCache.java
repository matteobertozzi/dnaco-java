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

package tech.dnaco.collections;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.util.BitUtil;
import tech.dnaco.util.ThreadUtil;

public class LruCache<TKey, TValue> {
  private final long maxSize;

  private final ReentrantLock lock = new ReentrantLock();

  private int[] buckets;
  private CacheItemNode[] entries;
  private CacheItemNode lruHead;
  private int count = 0;

  public LruCache(final int initialCapacity, final int maxSize) {
    this(initialCapacity, maxSize, null);
  }

  public LruCache(final int initialCapacity, final int maxSize, final Duration expiration) {
    this.maxSize = maxSize;

    final int capacity = BitUtil.nextPow2(Math.max(8, initialCapacity));
    this.entries = new CacheItemNode[capacity];
    this.buckets = new int[capacity];
    Arrays.fill(buckets, -1);

    lruHead = entries[0] = new CacheItemNode(0);
    for (int i = 1; i < entries.length; ++i) {
      final CacheItemNode node = entries[i] = new CacheItemNode(i);
      moveToLruTail(node);
    }
  }

  void dump() {
    System.out.println("DUMP ITEMS " + count);
    CacheItemNode node = lruHead;
    do {
      System.out.print(node.index + " ");
      node = entries[node.lruNext];
    } while (node != lruHead);
    System.out.println();

    node = entries[lruHead.lruPrev];
    do {
      System.out.print(node.index + " ");
      node = entries[node.lruPrev];
    } while (node != entries[lruHead.lruPrev]);
    System.out.println();
  }

  public int size() {
    lock.lock();
    try {
      return count;
    } finally {
      lock.unlock();
    }
  }

  public TValue get(final TKey key) {
    lock.lock();
    try {
      final CacheItemNode node = findEntry(key, hashCode(key));
      if (node == null) return null;

      moveToLruFront(node);
      return node.getValue();
    } finally {
      lock.unlock();
    }
  }

  public TValue put(final TKey key, final TValue value) {
    lock.lock();
    try {
      final int keyHash = hashCode(key);
      final CacheItemNode node = findEntry(key, keyHash);
      if (node == null) {
        insertNewEntry(keyHash, key, value);
        return null;
      }

      final TValue oldValue = node.getValue();
      node.set(value);
      moveToLruFront(node);
      return oldValue;
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    lock.lock();
    try {
      for (int i = 0; i < entries.length; ++i) {
        entries[i].clear();
      }
      Arrays.fill(buckets, -1);
    } finally {
      lock.unlock();
    }
  }

  public TValue evict(final TKey key) {
    lock.lock();
    try {
      return remove(key);
    } finally {
      lock.unlock();
    }
  }

  public void scanEvict(BiPredicate<TKey, TValue> predicate) {
    lock.lock();
    try {
      for (int i = 0; i < entries.length; ++i) {
        final CacheItemNode node = entries[i];
        if (node != null && predicate.test(node.getKey(), node.getValue())) {
          remove(node.getKey());
        }
      }
    } finally {
      lock.unlock();
    }
  }

  // ====================================================================================================
  //  HashTable related
  // ====================================================================================================
  private static int hashCode(final Object key) {
    return Objects.hashCode(key) & 0x7FFFFFFF;
  }

  private int targetBucket(final int keyHash) {
    return keyHash & (buckets.length - 1);
  }

  private CacheItemNode findEntry(final Object key, final int keyHash) {
    for (int i = buckets[targetBucket(keyHash)]; i >= 0; i = entries[i].hashNext) {
      final CacheItemNode entry = entries[i];
      //System.out.println(" -> " + i);
      if (entry.keyHash == keyHash && Objects.equals(entry.key, key)) {
        return entry;
      }
    }
    return null;
  }

  private long newFromFreeSlot = 0;
  private long newFromEviction = 0;
  private long newFromResize = 0;

  private void insertNewEntry(int keyHash, TKey key, TValue value) {
    // try to use an evicted item or if we are already at threshold we should evict a node
    if (entries[lruHead.lruPrev].isEmpty()) {
      // no-op
      newFromFreeSlot++;
    } else if (count >= maxSize) {
      remove(entries[lruHead.lruPrev].getKey());
      newFromEviction++;
    } else {
      final long startTime = System.nanoTime();
      final int newCapacity = entries.length << 1;
      if (newCapacity < 0) throw new IllegalStateException("HashMap too big size=" + entries.length);
      resize(newCapacity);
      newFromResize++;
      System.out.println("RESIZE " + newCapacity + " -> " + HumansUtil.humanTimeNanos(System.nanoTime() - startTime));
    }

    final CacheItemNode node = entries[lruHead.lruPrev];
    node.set(keyHash, key, value);
    moveToLruFront(node);

    final int bucketIndex = targetBucket(keyHash);
    node.hashNext = buckets[bucketIndex];
    buckets[bucketIndex] = node.index;
    count++;
  }

  private void resize(final int newSize) {
    // reset buckets
    if (newSize < 8192) buckets = new int[newSize];
    Arrays.fill(buckets, -1);

    // reassign entries
    final int entriesIndex = entries.length;
    entries = Arrays.copyOf(entries, newSize);
    for (int i = 0; i < entriesIndex; ++i) {
      final CacheItemNode node = entries[i];
      if (node.keyHash < 0) continue;
      final int bucket = targetBucket(node.keyHash);
      node.hashNext = buckets[bucket];
      buckets[bucket] = i;
    }

    // pre-alloc nodes
    for (int i = entriesIndex; i < newSize; ++i) {
      final CacheItemNode node = entries[i] = new CacheItemNode(i);
      moveToLruTail(node);
    }
  }

  private TValue remove(final TKey key) {
    final int hashCode = hashCode(key);
    final int bucket = targetBucket(hashCode);
    int last = -1;
    for (int i = buckets[bucket]; i >= 0; last = i, i = entries[i].hashNext) {
      final CacheItemNode node = entries[i];
      if (node.keyHash == hashCode && Objects.equals(node.key, key)) {
        if (last < 0) {
          buckets[bucket] = node.hashNext;
        } else {
          entries[last].hashNext = node.hashNext;
        }
        final TValue oldValue = node.getValue();
        node.clear();
        moveToLruTail(node);
        count--;
        return oldValue;
      }
    }
    return null;
  }

  // ====================================================================================================
  //  LRU related methods
  // ====================================================================================================
  private void moveToLruFront(final CacheItemNode node) {
    if (node == lruHead) return;

    node.unlink(entries);

    final CacheItemNode tail = entries[lruHead.lruPrev];
    node.lruNext = lruHead.index;
    node.lruPrev = tail.index;
    tail.lruNext = node.index;
    lruHead.lruPrev = node.index;
    lruHead = node;
  }

  private void moveToLruTail(final CacheItemNode node) {
    final CacheItemNode tail = entries[lruHead.lruPrev];
    if (node == tail) return;

    node.unlink(entries);
    node.lruNext = lruHead.index;
    node.lruPrev = tail.index;
    lruHead.lruPrev = node.index;
    tail.lruNext = node.index;
  }

  // ====================================================================================================
  //  Cache Item Node related
  // ====================================================================================================
  private static final class CacheItemNode {
    private final int index;

    private int keyHash;
    private int hashNext;
    private int lruPrev;
    private int lruNext;

    private Object key;
    private Object value;

    CacheItemNode(final int index) {
      this.index = index;
      this.lruPrev = index;
      this.lruNext = index;
    }

    void set(final int keyHash, final Object key, final Object value) {
      this.keyHash = keyHash;
      this.key = key;
      set(value);
    }

    void set(final Object value) {
      this.value = value;
    }

    void clear() {
      this.keyHash = -1;
      this.hashNext = -1;
      this.key = null;
      this.value = null;
    }

    boolean isEmpty() {
      return key == null;
    }

    void unlink(final CacheItemNode[] entries) {
      entries[lruPrev].lruNext = lruNext;
      entries[lruNext].lruPrev = lruPrev;
    }

    @SuppressWarnings("unchecked")
    public <T> T getKey() {
      return (T) key;
    }

    @SuppressWarnings("unchecked")
    public <T> T getValue() {
      return (T) value;
    }
  }

  public static void main(String[] args) {
    final int NLOOKUPS = 1000_000;

    final SecureRandom rand = new SecureRandom();

    for (int z = 0; z < 1024; ++z) {
      final int maxSize = BitUtil.nextPow2(rand.nextInt(1 << 20));
      final LruCache<String, String> lru = new LruCache<>(4, maxSize);
      final Thread[] thread = new Thread[16];
      for (int i = 0; i < thread.length; ++i) {
        thread[i] = new Thread(() -> {
          final SecureRandom localRand = new SecureRandom();
          for (int k = 0; k < NLOOKUPS; ++k) {
            final String key = "key-" + localRand.nextInt(4096 + 2048);
            if (lru.get(key) == null) lru.put(key, key);
          }
        });
      }

      final long startTime = System.nanoTime();
      for (int i = 0; i < thread.length; ++i) thread[i].start();
      for (int i = 0; i < thread.length; ++i) ThreadUtil.shutdown(thread[i]);
      final long elapsed = System.nanoTime() - startTime;
      System.out.println("[T] " + lru.size() + " -> " + HumansUtil.humanTimeNanos(elapsed)
        + " -> " + HumansUtil.humanRate((thread.length * NLOOKUPS) / (double)TimeUnit.NANOSECONDS.toSeconds(elapsed)));
    }
  }
}