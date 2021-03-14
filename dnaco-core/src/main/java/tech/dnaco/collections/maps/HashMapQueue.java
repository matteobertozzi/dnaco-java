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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.collections.iterators.AbstractIndexIterator;

public class HashMapQueue<K, V> implements Iterable<Map.Entry<K, V>> {
  private static final int DEFAULT_GROW_LENGTH = 16;
  private static final int MIN_CAPACITY = 16;
  private static final int MIN_BUCKETS = 32;

  private final Object[] buckets; // TODO: auto-scale buckets
  private Object[] entries;
  private long head;
  private long tail;

  private EntryIterator<K, V> iterator = null;

  public HashMapQueue() {
    this(MIN_BUCKETS, MIN_CAPACITY);
  }

  public HashMapQueue(final int nbuckets, final int capacity) {
    this.buckets = new Object[Math.max(MIN_BUCKETS, nbuckets)];
    this.entries = new Object[Math.max(MIN_CAPACITY, capacity)];
    this.head = 0;
    this.tail = 0;
  }

  // ==========================================================================================
  //  Utility methods
  // ==========================================================================================
  public boolean isEmpty() {
    return head == tail;
  }

  public boolean isNotEmpty() {
    return (tail - head) != 0;
  }

  public int size() {
    return (int)(tail - head);
  }

  public void clear() {
    Arrays.fill(buckets, null);
    Arrays.fill(entries, null);
    this.head = 0;
    this.tail = 0;
  }

  // ==========================================================================================
  //  Peek methods
  // ==========================================================================================
  public K peekKey() {
    final Map.Entry<K, V> entry = peek();
    return entry != null ? entry.getKey() : null;
  }

  public V peekValue() {
    final Map.Entry<K, V> entry = peek();
    return entry != null ? entry.getValue() : null;
  }

  public Map.Entry<K, V> peek() {
    return isEmpty() ? null : ArrayUtil.getItemAt(entries, getEntryIndex(head));
  }

  // ==========================================================================================
  //  Get methods
  // ==========================================================================================
  public V get(final K key) {
    final int bucketIndex = key.hashCode() % buckets.length;
    final Map.Entry<K, V> entry = getEntry(key, bucketIndex);
    return entry != null ? entry.getValue() : null;
  }

  public boolean containsKey(final K key) {
    final int bucketIndex = key.hashCode() % buckets.length;
    return getEntry(key, bucketIndex) != null;
  }

  public Map.Entry<K, V> getEntry(final K key, final int bucketIndex) {
    HMQEntry<K, V> node = ArrayUtil.getItemAt(buckets, bucketIndex);
    while (node != null) {
      if (key.equals(node.getKey())) {
        return node;
      }
      node = node.next;
    }
    return null;
  }

  private int getEntryIndex(final long index) {
    return (int) (index % entries.length);
  }

  // ==========================================================================================
  //  Pop related methods
  // ==========================================================================================
  public HMQEntry<K, V> pop() {
    if (isEmpty()) return null;

    final int index = getEntryIndex(head++);
    final HMQEntry<K, V> entry = ArrayUtil.getItemAt(entries, index);
    remove(entry, buckets, entry.getKey().hashCode() % buckets.length);
    entries[index] = null;
    return entry;
  }

  // ==========================================================================================
  //  Push related methods
  // ==========================================================================================
  public V push(final K key, final V value) {
    return put(key, value, false);
  }

  public V pushIfAbsent(final K key, final V value) {
    return put(key, value, true);
  }

  private V put(final K key, final V value, final boolean ifAbsent) {
    final int bucketIndex = key.hashCode() % buckets.length;
    final Map.Entry<K, V> entry = getEntry(key, bucketIndex);
    if (entry != null) {
      final V oldValue = entry.getValue();
      if (!ifAbsent) entry.setValue(value);
      return oldValue;
    }

    ensureCapacity();
    final HMQEntry<K, V> newEntry = new HMQEntry<>(key, value, null, null);
    entries[getEntryIndex(tail++)] = newEntry;
    add(newEntry, buckets, bucketIndex);
    return null;
  }

  private void ensureCapacity() {
    if (size() < entries.length) return;

    final int newCapacity = entries.length + ((entries.length < 64) ? entries.length : DEFAULT_GROW_LENGTH);
    final Object[] newEntries = new Object[newCapacity];
    Arrays.fill(buckets, null);

    int newTail = 0;
    for (long i = head; i < tail; ++i) {
      final HMQEntry<K, V> entry = ArrayUtil.getItemAt(entries, getEntryIndex(i));
      final int bucketIndex = entry.getKey().hashCode() % buckets.length;
      add(entry, buckets, bucketIndex);
      newEntries[newTail++] = entry;
    }

    this.entries = newEntries;
    this.head = 0;
    this.tail = newTail;
  }

  // ==========================================================================================
  // Entry link/unlink related
  // ==========================================================================================
  private static <K, V> void add(final HMQEntry<K, V> entry, final Object[] buckets, final int bucketIndex) {
    final HMQEntry<K, V> oldHead = ArrayUtil.getItemAt(buckets, bucketIndex);
    if (oldHead != null) oldHead.prev = entry;
    entry.next = oldHead;
    entry.prev = null;

    buckets[bucketIndex] = entry;
  }

  private static <K, V> void remove(final HMQEntry<K, V> entry, final Object[] buckets, final int bucketIndex) {
    if (entry.next != null) entry.next.prev = entry.prev;

    if (entry.prev != null) {
      entry.prev.next = entry.next;
    } else {
      buckets[bucketIndex] = entry.next;
    }

    entry.prev = null;
    entry.next = null;
  }

  // ==========================================================================================
  // Iterator related
  // ==========================================================================================
  @Override
  public Iterator<Map.Entry<K, V>> iterator() {
    if (iterator == null) {
      iterator = new EntryIterator<>(this);
    } else {
      iterator.reset();
    }
    return iterator;
  }

  public static final class HMQEntry<K, V> implements Map.Entry<K, V> {
    private K key;
    private V value;
    private HMQEntry<K, V> next;
    private HMQEntry<K, V> prev;

    private HMQEntry() {
      // no-op
    }

    private HMQEntry(final K key, final V value, final HMQEntry<K, V> next, final HMQEntry<K, V> prev) {
      this.key = key;
      this.value = value;
      this.next = next;
      this.prev = prev;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    public HMQEntry<K, V> set(final HMQEntry<K, V> entry) {
      this.key = entry.key;
      this.value = entry.value;
      return this;
    }

    @Override
    public V setValue(final V value) {
      final V oldValue = value;
      this.value = value;
      return oldValue;
    }
  }

  private static final class EntryIterator<K, V> extends AbstractIndexIterator<Map.Entry<K, V>> {
    private final HMQEntry<K, V> entry = new HMQEntry<>();
    private final HashMapQueue<K, V> queue;

    public EntryIterator(final HashMapQueue<K, V> queue) {
      super(queue.entries.length);
      this.queue = queue;
    }

    @Override
    protected Map.Entry<K, V> nextItemAt(final int index) {
      return entry.set(ArrayUtil.getItemAt(queue.entries, index));
    }

    @Override
    protected boolean isEmpty(final int index) {
      return queue.entries[index] == null;
    }
  }

  // ==========================================================================================
  // ToString
  // ==========================================================================================
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("{");
    for (long i = head; i < tail; ++i) {
      if (i > head) builder.append(", ");
      final HMQEntry<K, V> entry = ArrayUtil.getItemAt(entries, getEntryIndex(i));
      builder.append(entry.getKey());
      builder.append(": ");
      builder.append(entry.getValue());
    }
    builder.append("}");
    return builder.toString();
  }
}
