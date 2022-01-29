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

package tech.dnaco.collections.sets;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;

import tech.dnaco.collections.arrays.ArrayIterator;
import tech.dnaco.util.BitUtil;

public class HashIndexedArray<K> extends AbstractSet<K> {
  private final int[] buckets;
  private final int[] table; // hash|next|hash|next|...
  private final K[] keys;

  public HashIndexedArray(final K[] keys) {
    this.keys = keys;

    this.buckets = new int[tableSizeFor(keys.length + 8)];
    Arrays.fill(this.buckets, -1);

    this.table = new int[keys.length << 1];
    final int mask = buckets.length - 1;
    for (int i = 0, n = keys.length; i < n; ++i) {
      final int hashCode = hash(keys[i]);
      final int targetBucket = hashCode & mask;
      final int tableIndex = (i << 1);
      this.table[tableIndex] = hashCode;
      this.table[tableIndex + 1] = buckets[targetBucket];
      this.buckets[targetBucket] = i;
    }
  }

  public static void main(final String[] args) {
    final String[] keys = new String[] { "actual_shipm_start", "sales_document", "delivery", "shipment"};
    final HashIndexedArray<String> hash = new HashIndexedArray<>(keys);
    boolean result = false;
    for (int i = 0; i < 1; ++i) {
      for (int k = 0; k < keys.length; ++k) {
        result |= hash.contains(keys[k]);
      }
    }
    System.out.println(result);
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

  @Override
  @SuppressWarnings("unchecked")
  public boolean contains(final Object key) {
    return getIndex((K)key) >= 0;
  }

  @Override
  public Iterator<K> iterator() {
    return new ArrayIterator<>(keys);
  }

  public int getIndex(final Object key) {
    final int hashCode = hash(key);
    int index = buckets[hashCode & (buckets.length - 1)];
    while (index >= 0) {
      final int tableIndex = (index << 1);
      if (hashCode == table[tableIndex] && keys[index].equals(key)) {
        return index;
      }
      index = table[tableIndex + 1];
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

  private static int tableSizeFor(final int capacity) {
    final int MAXIMUM_CAPACITY = 1 << 30;
    final int n = BitUtil.nextPow2(capacity);
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n;
  }

  @Override
  public String toString() {
    return Arrays.toString(keys);
  }
}
