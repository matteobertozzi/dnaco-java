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

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.util.BitUtil;
import tech.dnaco.util.ThreadUtil;

public class LruCache<TKey, TValue> {
  private final long maxSize;
  private final long expirationIntervalNs;

  private final ReentrantLock lock = new ReentrantLock();

  private int[] buckets;
  private CacheItemNode[] entries;
  private CacheItemNode lruHead;
  private int count = 0;

  private long cacheHit = 0;
  private long cacheMiss = 0;
  private long cacheExpired = 0;

  public LruCache(final int initialCapacity, final int maxSize) {
    this(initialCapacity, maxSize, null);
  }

  public LruCache(final int initialCapacity, final int maxSize, final Duration expiration) {
    this.maxSize = maxSize;
    this.expirationIntervalNs = expiration != null ? expiration.toNanos() : -1;

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

    for (int i = 0; i < entries.length; ++i) {
      if (i > 0) System.out.print(" ");
      System.out.print(i + ":" + entries[i].getKey() + ":" + entries[i].getValue());
    }
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

  public LruCacheStats getStats() {
    lock.lock();
    try {
      return new LruCacheStats(cacheHit, cacheMiss, cacheExpired);
    } finally {
      lock.unlock();
    }
  }

  public TValue get(final TKey key) {
    final int keyHash = hashCode(key);
    lock.lock();
    try {
      final CacheItemNode node = findOrEvict(key, keyHash);
      if (node == null) return null;

      moveToLruFront(node);
      return node.getValue();
    } finally {
      lock.unlock();
    }
  }

  public TValue put(final TKey key, final TValue value) {
    final long expirationNs = expirationIntervalNs < 0 ? Long.MAX_VALUE : (System.nanoTime() + expirationIntervalNs);

    final int keyHash = hashCode(key);
    lock.lock();
    try {
      final CacheItemNode node = findEntry(key, keyHash);
      if (node == null) {
        insertNewEntry(keyHash, key, value, expirationNs);
        return null;
      }

      final TValue oldValue = node.getValue();
      node.set(value, expirationNs);
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
      count = 0;
    } finally {
      lock.unlock();
    }
  }

  public TValue evict(final TKey key) {
    final int keyHash = hashCode(key);
    lock.lock();
    try {
      return remove(key, keyHash);
    } finally {
      lock.unlock();
    }
  }

  public void scanEvict(final BiPredicate<TKey, TValue> predicate) {
    lock.lock();
    try {
      for (int i = 0; i < entries.length; ++i) {
        final CacheItemNode node = entries[i];
        if (node != null && !node.isEmpty() && predicate.test(node.getKey(), node.getValue())) {
          remove(node.getKey(), node.keyHash);
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
      if (entry.keyHash == keyHash && Objects.equals(entry.key, key)) {
        return entry;
      }
    }
    return null;
  }

  private long newFromFreeSlot = 0;
  private long newFromEviction = 0;
  private long newFromResize = 0;

  private void insertNewEntry(final int keyHash, final TKey key, final TValue value, final long expirationNs) {
    // try to use an evicted item or if we are already at threshold we should evict a node
    if (entries[lruHead.lruPrev].isEmpty()) {
      // no-op
      newFromFreeSlot++;
    } else if (count >= maxSize) {
      final CacheItemNode entry = entries[lruHead.lruPrev];
      remove(entry.getKey(), entry.keyHash);
      newFromEviction++;
    } else {
      final long startTime = System.nanoTime();
      final int newCapacity = entries.length << 1;
      if (newCapacity < 0) throw new IllegalStateException("LruCacheMap too big. size=" + entries.length);
      resize(newCapacity);
      newFromResize++;
      Logger.debug("RESIZE {} -> {}", newCapacity, HumansUtil.humanTimeNanos(System.nanoTime() - startTime));
    }

    final CacheItemNode node = entries[lruHead.lruPrev];
    node.set(keyHash, key, value, expirationNs);
    moveToLruFront(node);

    final int bucketIndex = targetBucket(keyHash);
    node.hashNext = buckets[bucketIndex];
    buckets[bucketIndex] = node.index;
    count++;
  }

  private static final int MAX_BUCKETS_COUNT = 16 << 10;
  private void resize(final int newSize) {
    // resize buckets up to MAX_BUCKETS_COUNT
    if (buckets.length < MAX_BUCKETS_COUNT) {
      buckets = new int[Math.min(newSize, MAX_BUCKETS_COUNT)];
    }
    // reset buckets
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

  private TValue remove(final TKey key, final int hashCode) {
    final int bucket = targetBucket(hashCode);
    int last = -1;
    for (int i = buckets[bucket]; i >= 0; last = i, i = entries[i].hashNext) {
      final CacheItemNode node = entries[i];
      if (node.keyHash == hashCode && Objects.equals(node.key, key)) {
        return removeNode(node, bucket, last);
      }
    }
    return null;
  }

  private CacheItemNode findOrEvict(final TKey key, final int hashCode) {
    final int bucket = targetBucket(hashCode);
    int last = -1;
    for (int i = buckets[bucket]; i >= 0; last = i, i = entries[i].hashNext) {
      final CacheItemNode node = entries[i];
      if (node.keyHash == hashCode && Objects.equals(node.key, key)) {
        if (expirationIntervalNs > 0 && node.isExpired(System.nanoTime())) {
          removeNode(node, bucket, last);
          cacheExpired++;
          cacheMiss++;
          return null;
        }
        cacheHit++;
        return node;
      }
    }
    cacheMiss++;
    return null;
  }

  private TValue removeNode(final CacheItemNode node, final int bucket, final int last) {
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

    private long expirationNs;
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

    public boolean isExpired(final long nowNs) {
      return nowNs > expirationNs;
    }

    void set(final int keyHash, final Object key, final Object value, final long expirationNs) {
      this.keyHash = keyHash;
      this.key = key;
      set(value, expirationNs);
    }

    void set(final Object value, final long expirationNs) {
      this.value = value;
      this.expirationNs = expirationNs;
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

  public static final class LruCacheStats {
    private final long cacheHit;
    private final long cacheMiss;
    private final long cacheExpired;

    private LruCacheStats(final long cacheHit, final long cacheMiss, final long cacheExpired) {
      this.cacheHit = cacheHit;
      this.cacheMiss = cacheMiss;
      this.cacheExpired = cacheExpired;
    }

    public int getCacheHitRatio() {
      return Math.round(100 * (((float)cacheHit) / (cacheHit + cacheMiss)));
    }

    public int cacheMissRatio() {
      return Math.round(100 * (((float)cacheMiss) / (cacheHit + cacheMiss)));
    }

    public long getCacheHit() {
      return cacheHit;
    }

    public long getCacheMiss() {
      return cacheMiss;
    }

    public long getCacheExpired() {
      return cacheExpired;
    }

    @Override
    public String toString() {
      return "LruCacheStats [cacheHit=" + cacheHit + ", cacheMiss=" + cacheMiss + ", cacheExpired=" + cacheExpired + "]";
    }
  }

  public static void main(final String[] args) {
    final LruCache<String, String> lru = new LruCache<>(8, 8);
    for (int i = 0; i < 16; ++i) lru.put("K" + i, "V" + i);
    lru.dump();
    lru.clear();
    lru.dump();
    for (int i = 16; i < 32; ++i) lru.put("K" + i, "V" + i);
    lru.dump();
    //testPerf();
  }

  private static void testPerf() {
    final int NLOOKUPS = 1000_000;

    final SecureRandom rand = new SecureRandom();

    for (int z = 0; z < 1024; ++z) {
      final int maxSize = BitUtil.nextPow2(rand.nextInt(1 << 20));
      final LruCache<String, String> lru = new LruCache<>(4, maxSize, Duration.ofSeconds(1));
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
      System.out.println("[T] " + lru.size()
        + " cacheHit=" + lru.cacheHit
        + " cacheMiss=" + lru.cacheMiss
        + " cacheExpired=" + lru.cacheExpired
        + " -> " + HumansUtil.humanTimeNanos(elapsed)
        + " -> " + HumansUtil.humanRate((thread.length * NLOOKUPS) / (double)TimeUnit.NANOSECONDS.toSeconds(elapsed)));
    }
  }
}