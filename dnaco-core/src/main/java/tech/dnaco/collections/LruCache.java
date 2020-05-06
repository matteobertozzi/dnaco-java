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

import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.util.BitUtil;
import tech.dnaco.util.ThreadUtil;

public class LruCache<TKey, TValue> {
  interface CacheItemSizeEstimator<TKey, TValue> {
    long itemSizeEstimate (TKey key, TValue value);
  }

  private final CacheItemSizeEstimator<TKey, TValue> estimator;
  private final long maxSize;

  private final ReentrantLock lock = new ReentrantLock();

  private CacheItemNode[] entries = null;
  private int[] buckets = null;
  private int count = 0;

  private CacheItemNode lruHead;
  private long estimateSize = 0;

  public LruCache(final int initialCapacity, final int maxSize, final CacheItemSizeEstimator<TKey, TValue> estimator) {
    this.estimator = estimator;
    this.maxSize = maxSize;

    final int capacity = BitUtil.nextPow2(Math.max(8, initialCapacity));
    this.entries = new CacheItemNode[capacity];
    this.buckets = new int[capacity];
    Arrays.fill(buckets, -1);

    lruHead = entries[0] = new CacheItemNode(0);
    for (int i = 1; i < entries.length; ++i) {
      final CacheItemNode node = new CacheItemNode(i);
      moveToLruTail(node);
      entries[i] = node;
    }
  }

  void dump() {
    System.out.println("DUMP ITEMS " + count);
    CacheItemNode node = lruHead;
    do {
      System.out.print(node.index + " ");
      node = node.lruNext;
    } while (node != lruHead);
    System.out.println();

    node = lruHead.lruPrev;
    do {
      System.out.print(node.index + " ");
      node = node.lruPrev;
    } while (node != lruHead.lruPrev);
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
        estimateSize += estimator.itemSizeEstimate(key, value);
        return null;
      }

      final TValue oldValue = node.getValue();
      estimateSize -= estimator.itemSizeEstimate(key, oldValue);
      node.set(value);
      estimateSize += estimator.itemSizeEstimate(key, value);
      moveToLruFront(node);
      return oldValue;
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
    if (lruHead.lruPrev.isEmpty()) {
      // no-op
      newFromFreeSlot++;
    } else if (estimateSize >= maxSize) {
      remove(lruHead.lruPrev.getKey());
      newFromEviction++;
    } else {
      final int newCapacity = entries.length << 1;
      if (newCapacity < 0) throw new IllegalStateException("HashMap too big size=" + entries.length);
      resize(newCapacity);
      newFromResize++;
    }

    final CacheItemNode node = lruHead.lruPrev;
    node.set(keyHash, key, value);
    moveToLruFront(node);

    final int bucketIndex = targetBucket(keyHash);
    node.hashNext = buckets[bucketIndex];
    buckets[bucketIndex] = node.index;
    count++;
  }

  private void resize(final int newSize) {
    // reset buckets
    if (newSize >= 8192) buckets = new int[newSize];
    Arrays.fill(buckets, -1);

    // reassign entries
    final int entriesIndex = entries.length;
    final CacheItemNode[] newEntries = Arrays.copyOf(entries, newSize);
    for (int i = 0; i < entriesIndex; ++i) {
      if (newEntries[i].keyHash < 0) continue;
      final int bucket = targetBucket(newEntries[i].keyHash);
      newEntries[i].hashNext = buckets[bucket];
      buckets[bucket] = i;
    }
    this.entries = newEntries;

    // pre-alloc nodes
    for (int i = entriesIndex; i < entries.length; ++i) {
      final CacheItemNode node = new CacheItemNode(i);
      moveToLruTail(node);
      entries[i] = node;
    }
  }

  public TValue remove(final TKey key) {
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
        estimateSize -= estimator.itemSizeEstimate(key, oldValue);
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

    node.unlink();

    final CacheItemNode tail = lruHead.lruPrev;
    node.lruNext = lruHead;
    node.lruPrev = tail;
    tail.lruNext = node;
    lruHead.lruPrev = node;
    lruHead = node;
  }

  private void moveToLruTail(final CacheItemNode node) {
    final CacheItemNode tail = lruHead.lruPrev;
    if (node == tail) return;

    node.unlink();
    node.lruNext = lruHead;
    node.lruPrev = tail;
    lruHead.lruPrev = node;
    tail.lruNext = node;
  }

  // ====================================================================================================
  //  Cache Item Node related
  // ====================================================================================================
  private static final class CacheItemNode {
    private final int index;

    private int keyHash;
    private int hashNext;
    private CacheItemNode lruPrev;
    private CacheItemNode lruNext;

    private Object key;
    private Object value;

    CacheItemNode(final int index) {
      this.index = index;
      this.lruPrev = this;
      this.lruNext = this;
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

    void unlink() {
      lruPrev.lruNext = lruNext;
      lruNext.lruPrev = lruPrev;
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
    final LruCache<String, String> lru = new LruCache<>(4, 32, (k, v) -> 1);
    Thread[] thread = new Thread[16];
    for (int i = 0; i < thread.length; ++i) {
      thread[i] = new Thread(() -> {
        final Random rand = new Random();
        for (int k = 0; k < NLOOKUPS; ++k) {
          final String key = "key-" + rand.nextInt(32 + 16);
          if (lru.get(key) == null) lru.put(key, key);
        }
      });
    }

    final long startTime = System.nanoTime();
    for (int i = 0; i < thread.length; ++i) thread[i].start();
    for (int i = 0; i < thread.length; ++i) ThreadUtil.shutdown(thread[i]);
    final long elapsed = System.nanoTime() - startTime;
    System.out.println("[T] " + HumansUtil.humanTimeNanos(elapsed)
      + " -> " + HumansUtil.humanRate((thread.length * NLOOKUPS) / (double)TimeUnit.NANOSECONDS.toSeconds(elapsed)));
    lru.dump();
    System.out.println(lru.estimateSize);
  }
}