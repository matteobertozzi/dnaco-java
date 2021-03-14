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

package tech.dnaco.collections.maps;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import tech.dnaco.util.BitUtil;

public class OrderedHashMap<K, V> extends AbstractMap<K, V> {
  private static final int MIN_CAPACITY = 16;

  private OrderedEntry[] entries;
  private int[] buckets;
  private int entriesIndex;
  private int freeList;
  private int count;

  public OrderedHashMap() {
    this(0);
  }

  public OrderedHashMap(final int initialSize) {
    final int capacity = initialSize > 0 ? BitUtil.nextPow2(initialSize) : 0;
    this.entriesIndex = 0;
    this.freeList = -1;
    this.entries = new OrderedEntry[capacity];
    this.buckets = new int[capacity];
    this.count = 0;

    Arrays.fill(buckets, -1);
  }

  @Override
  public int size() {
    return count;
  }

  @Override
  public boolean isEmpty() {
    return count == 0;
  }

  public boolean isNotEmpty() {
    return count != 0;
  }

  @Override
  public boolean containsKey(final Object key) {
    return findEntry(key) != null;
  }

  @Override
  public V get(final Object key) {
    final OrderedEntry entry = findEntry(key);
    return entry != null ? getEntryValue(entry) : null;
  }

  @Override
  public V put(final K key, final V value) {
    final int hashCode = hash(key);
    final OrderedEntry entry = findEntry(key, hashCode);
    if (entry != null) {
      final V oldValue = getEntryValue(entry);
      entry.value = value;
      return oldValue;
    }

    insertNewEntry(hashCode, key, value);
    return null;
  }

  @Override
  public V remove(final Object key) {
    final int hashCode = hash(key);
    final int bucket = hashCode & (buckets.length - 1);
    int last = -1;
    for (int i = buckets[bucket]; i >= 0; last = i, i = entries[i].next) {
      final OrderedEntry entry = entries[i];
      if (entry.hash == hashCode && keyEquals(entry.key, key)) {
        if (last < 0) {
          buckets[bucket] = entry.next;
        } else {
          entries[last].next = entry.next;
        }
        final V oldValue = getEntryValue(entry);
        entry.hash = -1;
        entry.next = freeList;
        entry.key = null;
        entry.value = null;
        freeList = i;
        count--;
        return oldValue;
      }
    }
    return null;
  }

  public List<K> keys() {
    final ArrayList<K> keys = new ArrayList<>(count);
    for (int i = 0; i < entriesIndex; ++i) {
      if (entries[i] != null && entries[i].key != null) {
        keys.add(getEntryKey(entries[i]));
      }
    }
    return keys;
  }

  public List<V> values() {
    final ArrayList<V> vals = new ArrayList<>(count);
    for (int i = 0; i < entriesIndex; ++i) {
      if (entries[i] != null && entries[i].key != null) {
        vals.add(getEntryValue(entries[i]));
      }
    }
    return vals;
  }

  private void insertNewEntry(final int hashCode, final K key, final V value) {
    int targetBucket = hashCode & (buckets.length - 1);
    final int index;
    if (freeList >= 0) {
      index = freeList;
      freeList = entries[index].next;
    } else {
      if (entriesIndex == entries.length) {
        resize();
        targetBucket = hashCode & (buckets.length - 1);
      }
      index = entriesIndex++;
    }

    writeEntry(index, hashCode, buckets[targetBucket], key, value);
    buckets[targetBucket] = index;
    count++;
  }

  private void resize() {
    final int newCapacity = (entriesIndex != 0) ? entriesIndex << 1 : MIN_CAPACITY;
    if (newCapacity < 0) {
      throw new IllegalStateException("Map too big size=" + entriesIndex);
    }
    resize(newCapacity);
  }

  private void resize(final int newSize) {
    final int[] newBuckets = new int[newSize];
    Arrays.fill(newBuckets, -1);

    final OrderedEntry[] newEntries = new OrderedEntry[newSize];
    System.arraycopy(entries, 0, newEntries, 0, entriesIndex);

    final int mask = newSize - 1;
    for (int i = 0; i < entriesIndex; i++) {
      if (newEntries[i].hash >= 0) {
        final int bucket = newEntries[i].hash & mask;
        newEntries[i].next = newBuckets[bucket];
        newBuckets[bucket] = i;
      }
    }
    this.buckets = newBuckets;
    this.entries = newEntries;
  }

  private OrderedEntry findEntry(final Object key) {
    return findEntry(key, hash(key));
  }

  private OrderedEntry findEntry(final Object key, final int hashCode) {
    if (buckets.length == 0) return null;

    for (int i = buckets[hashCode & buckets.length - 1]; i >= 0; i = entries[i].next) {
      final OrderedEntry entry = entries[i];
      if (entry.hash == hashCode && keyEquals(entry.key, key)) {
        return entry;
      }
    }
    return null;
  }

  protected boolean keyEquals(final Object a, final Object b) {
    return Objects.equals(a, b);
  }

  private static int hash(final Object key) {
    return Objects.hashCode(key) & 0x7FFFFFFF;
  }

  private void writeEntry(final int index, final int hashCode, final int next, final K key, final V value) {
    final OrderedEntry entry = getOrCreateEntry(index);
    entry.hash = hashCode;
    entry.next = next;
    entry.key = key;
    entry.value = value;
  }

  private OrderedEntry getOrCreateEntry(final int index) {
    final OrderedEntry entry = entries[index];
    if (entry != null) return entry;

    entries[index] = new OrderedEntry();
    return entries[index];
  }

  @SuppressWarnings("unchecked")
  private V getEntryValue(final OrderedEntry entry) {
    return (V) entry.value;
  }

  @SuppressWarnings("unchecked")
  private K getEntryKey(final OrderedEntry entry) {
    return (K) entry.key;
  }

  @Override
  public String toString() {
    if (count == 0) {
      return "{}";
    }

    final StringBuilder dict = new StringBuilder();
    dict.append("{");
    for (int i = 0; i < entriesIndex; ++i) {
      if (i > 0) dict.append(", ");
      final OrderedEntry entry = entries[i];
      dict.append(entry.key).append(":").append(entry.value);
    }
    dict.append("}");

    return dict.toString();
  }

  private static final class OrderedEntry {
    private int hash;
    private int next;
    private Object key;
    private Object value;
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return new EntrySet<>(this);
  }

  private static final class EntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
    private final OrderedHashMap<K, V> map;

    private EntrySet(final OrderedHashMap<K, V> map) {
      this.map = map;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
      return new EntryIterator<>(map);
    }

    @Override
    public int size() {
      return map.size();
    }
  }

  private static final class EntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {
    private final OrderedHashMap<K, V> map;
    private int index;
    private int count;

    private EntryIterator(final OrderedHashMap<K, V> map) {
      this.map = map;
      this.index = 0;
      this.count = 0;
    }

    @Override
    public boolean hasNext() {
      return count < map.count;
    }

    @Override
    public Entry<K, V> next() {
      if (count >= map.count) {
        throw new NoSuchElementException();
      }

      OrderedEntry entry = map.entries[index++];
      while (entry == null || entry.key == null) {
        entry = map.entries[index++];
      }

      count++;
      return new SimpleImmutableEntry<>(map.getEntryKey(entry), map.getEntryValue(entry));
    }
  }
}
