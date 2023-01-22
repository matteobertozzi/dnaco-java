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

package tech.dnaco.dispatcher.message;

import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.collections.maps.MapUtil;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.BitUtil;

public class MessageMetadataMap implements MessageMetadata {
  private static final int MIN_CAPACITY = 8;

  private MetadataEntry[] entries;
  private int[] buckets;
  private int count;

  public MessageMetadataMap() {
    this(MIN_CAPACITY);
  }

  public MessageMetadataMap(final int initialCapacity) {
    final int capacity = BitUtil.nextPow2(Math.max(MIN_CAPACITY, initialCapacity));

    this.entries = new MetadataEntry[capacity];
    this.buckets = new int[capacity];
    this.count = 0;

    Arrays.fill(buckets, -1);
  }

  public MessageMetadataMap(final Collection<Entry<String, String>> entries) {
    this(entries.size());
    addAll(entries);
  }

  public MessageMetadataMap(final Map<String, String> headers) {
    this(headers.entrySet());
  }

  public MessageMetadataMap(final MessageMetadata headers) {
    this(headers.size());
    headers.forEach(this::add);
  }

  public static MessageMetadataMap fromKeyValues(final String[] kvs) {
    if (ArrayUtil.isEmpty(kvs)) return new MessageMetadataMap();

    final MessageMetadataMap headers = new MessageMetadataMap(kvs.length / 2);
    for (int i = 0; i < kvs.length; i += 2) {
      headers.add(kvs[i], kvs[i + 1]);
    }
    return headers;
  }

  public static MessageMetadataMap fromMultiMap(final Map<String, List<String>> multiMap) {
    if (MapUtil.isEmpty(multiMap)) return new MessageMetadataMap();

    final MessageMetadataMap headers = new MessageMetadataMap(multiMap.size());
    for (final Entry<String, List<String>> entry: multiMap.entrySet()) {
      for (final String value: entry.getValue()) {
        headers.add(entry.getKey(), value);
      }
    }
    return headers;
  }

  public static MessageMetadataMap single(final String key, final String value) {
    final MessageMetadataMap headers = new MessageMetadataMap();
    headers.add(key, value);
    return headers;
  }

  public static MessageMetadataMap fromFormUrlEncoded(final String formData) {
    if (StringUtil.isEmpty(formData)) {
      return new MessageMetadataMap();
    }

    final MessageMetadataMap metadata = new MessageMetadataMap();
    int index = formData.indexOf('?') + 1;
    while (index < formData.length()) {
      final int keyEndIndex = formData.indexOf('=', index);
      int valEndIndex = formData.indexOf('&', keyEndIndex + 1);
      if (valEndIndex < 0) valEndIndex = formData.length();

      final String key = URLDecoder.decode(formData.substring(index, keyEndIndex), StandardCharsets.UTF_8);
      final String val = URLDecoder.decode(formData.substring(keyEndIndex + 1, valEndIndex), StandardCharsets.UTF_8);
      metadata.add(key, val);

      index = valEndIndex + 1;
    }
    return metadata;
  }

  public static void main(final String[] args) throws Exception {
    final MessageMetadataMap x = fromFormUrlEncoded("c=20&a=10&b=10");
    System.out.println(Arrays.toString(x.toStringArray()));
  }

  public int size() {
    return count;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  public boolean containsKey(final Object key) {
    return findEntry((String)key) != null;
  }

  public MetadataEntry[] rawEntries() {
    return entries;
  }

  public List<Map.Entry<String, String>> entries() {
    if (isEmpty()) return Collections.emptyList();

    final ArrayList<Map.Entry<String, String>> headers = new ArrayList<>(count);
    for (int i = 0; i < count; ++i) {
      final MetadataEntry entry = entries[i];
      headers.add(entry);
    }
    return headers;
  }

  @Override
  public void forEach(final BiConsumer<? super String, ? super String> action) {
    for (int i = 0; i < count; ++i) {
      final MetadataEntry entry = entries[i];
      action.accept(entry.getKey(), entry.value);
    }
  }

  public Set<Entry<String, String>> entrySet() {
    return new HashSet<>(entries());
  }

  public String get(final Object key) {
    final MetadataEntry entry = findEntry((String) key);
    return entry != null ? entry.value : null;
  }

  @Override
  public String get(final String key) {
    final MetadataEntry entry = findEntry(key);
    return entry != null ? entry.value : null;
  }

  public List<String> getList(final String key) {
    final String lowerKey = key.toLowerCase();
    final int hashCode = Objects.hashCode(lowerKey) & 0x7FFFFFFF;

    MetadataEntry entry = findEntry(lowerKey, hashCode);
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

  private MetadataEntry findEntry(final String key) {
    final String lowerKey = key.toLowerCase();
    final int hashCode = Objects.hashCode(lowerKey) & 0x7FFFFFFF;
    return findEntry(lowerKey, hashCode);
  }

  private MetadataEntry findEntry(final String key, final int hashCode) {
    for (int i = buckets[hashCode % buckets.length]; i >= 0; i = entries[i].next) {
      final MetadataEntry entry = entries[i];
      if (entry.hash == hashCode && StringUtil.equals(entry.key, key)) {
        return entry;
      }
    }
    return null;
  }

  public String put(final String key, final String value) {
    return set(key, value);
  }

  public String set(final String key, final boolean value) {
    return set(key, String.valueOf(value));
  }

  public String set(final String key, final long value) {
    return set(key, String.valueOf(value));
  }

  public String set(final String key, final URL value) {
    return set(key, String.valueOf(value));
  }

  public String set(final String key, final ZonedDateTime dateTime) {
    return set(key, DateTimeFormatter.RFC_1123_DATE_TIME.format(dateTime));
  }

  public String set(final String key, final String value) {
    if (StringUtil.isEmpty(value)) return null;

    final String lowerKey = key.toLowerCase();
    final int hashCode = Objects.hashCode(lowerKey) & 0x7FFFFFFF;
    final MetadataEntry entry = findEntry(lowerKey, hashCode);
    if (entry != null) {
      final String oldValue = entry.value;
      entry.value = value;
      return oldValue;
    }

    insertNewEntry(hashCode, lowerKey, value);
    return null;
  }

  public MessageMetadataMap add(final String key, final long value) {
    return add(key, String.valueOf(value));
  }

  public MessageMetadataMap add(final String key, final boolean value) {
    return add(key, String.valueOf(value));
  }

  public MessageMetadataMap add(final String key, final String value) {
    if (StringUtil.isEmpty(value)) return this;

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
    final MetadataEntry entry = new MetadataEntry();
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

    final MetadataEntry[] newEntries = new MetadataEntry[newSize];
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

  public static final class MetadataEntry implements Map.Entry<String, String> {
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
      return "MetadataEntry [hash=" + hash + ", key=" + key + ", next=" + next + ", value=" + value + "]";
    }
  }

  @Override
  public String toString() {
    int count = 0;
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < entries.length; ++i) {
      if (entries[i] == null) continue;

      if (count++ > 0) builder.append(", ");
      builder.append(entries[i].key);
      builder.append(":");
      builder.append(entries[i].value);
    }
    return builder.toString();
  }
}
