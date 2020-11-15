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

import tech.dnaco.util.BitUtil;

public class HashIndexedArray<K> {
  private final int[] buckets;
  private final int[] hashes;
  private final int[] next;
  private final K[] keys;

  public HashIndexedArray(final K[] keys) {
    this.keys = keys;

    this.buckets = new int[tableSizeFor(keys.length + 8)];
    Arrays.fill(this.buckets, -1);
    this.hashes = new int[keys.length];
    this.next = new int[keys.length];

    final int nBuckets = buckets.length - 1;
    for (int i = 0, n = keys.length; i < n; ++i) {
      final int hashCode = hash(keys[i]);
      final int targetBucket = hashCode & nBuckets;
      this.hashes[i] = hashCode;
      this.next[i] = buckets[targetBucket];
      this.buckets[targetBucket] = i;
    }
  }

  public int size() {
    return keys.length;
  }

  public K[] keySet() {
    return keys;
  }

  public K get(final int index) {
    return keys[index];
  }

  public boolean contains(final K key) {
    return getIndex(key) >= 0;
  }

  public int getIndex(final K key) {
    final int hashCode = hash(key);
    int index = buckets[hashCode & (buckets.length - 1)];
    while (index >= 0) {
      if (hashCode == hashes[index] && keys[index].equals(key)) {
        return index;
      }
      index = next[index];
    }
    return -1;
  }

  private static int hash(final Object key) {
    int h = key.hashCode() & 0x7fffffff;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = (h >>> 16) ^ h;
    return h & 0x7fffffff;
  }

  private static int tableSizeFor(final int cap) {
    final int MAXIMUM_CAPACITY = 1 << 30;
    final int n = BitUtil.nextPow2(cap);
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(keys);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (!(obj instanceof HashIndexedArray)) return false;

    @SuppressWarnings("unchecked")
    final HashIndexedArray<K> other = (HashIndexedArray<K>) obj;
    if (keys.length != other.size()) return false;

    for (int i = 0; i < keys.length; ++i) {
      if (!other.contains(keys[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return Arrays.toString(keys);
  }
}
