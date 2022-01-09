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

package tech.dnaco.net.message;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import tech.dnaco.strings.StringConverter;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.BitUtil;

public class DnacoMetadataMap extends AbstractMap<String, String> {
  private static final int MIN_CAPACITY = 8;

  private HeaderEntry[] entries;
  private int[] buckets;
  private int count;

  public DnacoMetadataMap() {
    this(MIN_CAPACITY);
  }

  public DnacoMetadataMap(final int initialCapacity) {
    final int capacity = BitUtil.nextPow2(Math.max(MIN_CAPACITY, initialCapacity));

    this.entries = new HeaderEntry[capacity];
    this.buckets = new int[capacity];
    this.count = 0;

    Arrays.fill(buckets, -1);
  }

  public DnacoMetadataMap(final Collection<Entry<String, String>> entries) {
    this(entries.size());
    addAll(entries);
  }

  public DnacoMetadataMap(final Map<String, String> headers) {
    this(headers.entrySet());
  }

  public static DnacoMetadataMap fromMultiMap(final Map<String, List<String>> multiMap) {
    final DnacoMetadataMap headers = new DnacoMetadataMap(multiMap.size());
    for (final Entry<String, List<String>> entry: multiMap.entrySet()) {
      for (final String value: entry.getValue()) {
        headers.add(entry.getKey(), value);
      }
    }
    return headers;
  }

  public static DnacoMetadataMap single(final String key, final String value) {
    final DnacoMetadataMap headers = new DnacoMetadataMap();
    headers.add(key, value);
    return headers;
  }

  @Override
  public int size() {
    return count;
  }

  @Override
  public boolean isEmpty() {
    return count == 0;
  }

  @Override
  public boolean containsKey(final Object key) {
    return findEntry((String)key) != null;
  }

  public List<Map.Entry<String, String>> entries() {
    if (isEmpty()) return Collections.emptyList();

    final ArrayList<Map.Entry<String, String>> headers = new ArrayList<>(count);
    for (int i = 0; i < count; ++i) {
      final HeaderEntry entry = entries[i];
      headers.add(entry);
    }
    return headers;
  }

  @Override
  public void forEach(final BiConsumer<? super String, ? super String> action) {
    for (int i = 0; i < count; ++i) {
      final HeaderEntry entry = entries[i];
      action.accept(entry.getKey(), entry.value);
    }
  }

  @Override
  public Set<Entry<String, String>> entrySet() {
    return new HashSet<>(entries());
  }

  @Override
  public String get(final Object key) {
    final HeaderEntry entry = findEntry((String) key);
    return entry != null ? entry.value : null;
  }

  public int getInt(final String key, final int defaultValue) {
    return StringConverter.toInt(get(key), defaultValue);
  }

  public List<String> getList(final String key) {
    final String lowerKey = key.toLowerCase();
    final int hashCode = Objects.hashCode(lowerKey) & 0x7FFFFFFF;

    HeaderEntry entry = findEntry(lowerKey, hashCode);
    if (entry == null) return Collections.emptyList();

    final String firstValue = entry.value;
    List<String> values = null;
    while (entry.next >= 0) {
      entry = entries[entry.next];
      if (entry.hash == hashCode && StringUtil.equals(entry.key, key)) {
        if (values == null) {
          values = new ArrayList<>();
          values.add(firstValue);
        }
        values.add(entry.value);
      }
    }
    return values != null ? values : Collections.singletonList(firstValue);
  }

  private HeaderEntry findEntry(final String key) {
    final String lowerKey = key.toLowerCase();
    final int hashCode = Objects.hashCode(lowerKey) & 0x7FFFFFFF;
    return findEntry(lowerKey, hashCode);
  }

  private HeaderEntry findEntry(final String key, final int hashCode) {
    for (int i = buckets[hashCode % buckets.length]; i >= 0; i = entries[i].next) {
      final HeaderEntry entry = entries[i];
      if (entry.hash == hashCode && StringUtil.equals(entry.key, key)) {
        return entry;
      }
    }
    return null;
  }

  @Override
  public String put(final String key, final String value) {
    return set(key, value);
  }

  public String set(final String key, final long value) {
    return set(key, String.valueOf(value));
  }

  public String set(final String key, final String value) {
    final String lowerKey = key.toLowerCase();
    final int hashCode = Objects.hashCode(lowerKey) & 0x7FFFFFFF;
    final HeaderEntry entry = findEntry(lowerKey, hashCode);
    if (entry != null) {
      final String oldValue = entry.value;
      entry.value = value;
      return oldValue;
    }

    insertNewEntry(hashCode, lowerKey, value);
    return null;
  }

  public DnacoMetadataMap add(final String key, final long value) {
    return add(key, String.valueOf(value));
  }

  public DnacoMetadataMap add(final String key, final String value) {
    final String lowerKey = key.toLowerCase();
    final int hashCode = Objects.hashCode(lowerKey) & 0x7FFFFFFF;
    insertNewEntry(hashCode, lowerKey, value);
    return this;
  }

  public void addAll(final Collection<Entry<String, String>> entries) {
    for (final Entry<String, String> entry: entries) {
      add(entry.getKey(), entry.getValue());
    }
  }

  private void insertNewEntry(final int hashCode, final String key, final String value) {
    int targetBucket = hashCode % buckets.length;

    if (count == entries.length) {
      final int newCapacity = count << 1;
      if (newCapacity < 0) throw new IllegalStateException("HashMap too big size=" + count);
      resize(newCapacity);

      targetBucket = hashCode % buckets.length;
    }

    final int index = count++;
    final HeaderEntry entry = new HeaderEntry();
    entries[index] = entry;
    entry.hash = hashCode;
    entry.next = buckets[targetBucket];
    entry.key = key;
    entry.value = value;
    buckets[targetBucket] = index;
  }

  private void resize(final int newSize) {
    final int[] newBuckets = new int[newSize];
    Arrays.fill(newBuckets, -1);

    final HeaderEntry[] newEntries = new HeaderEntry[newSize];
    System.arraycopy(entries, 0, newEntries, 0, count);
    for (int i = 0; i < count; i++) {
      if (newEntries[i].hash >= 0) {
        final int bucket = newEntries[i].hash % newSize;
        newEntries[i].next = newBuckets[bucket];
        newBuckets[bucket] = i;
      }
    }
    this.buckets = newBuckets;
    this.entries = newEntries;
  }

  private static final class HeaderEntry implements Map.Entry<String, String> {
    private int hash;
    private int next;
    private String key;
    private String value;

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public String getValue() {
      return value;
    }

    @Override
    public String setValue(final String value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "HeaderEntry [hash=" + hash + ", key=" + key + ", next=" + next + ", value=" + value + "]";
    }
  }
}
