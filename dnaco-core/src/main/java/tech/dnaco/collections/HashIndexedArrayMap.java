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
import java.util.Map;
import java.util.function.Function;

public class HashIndexedArrayMap<K, V> {
  private final HashIndexedArray<K> keyIndex;
  private final V[] values;

  public HashIndexedArrayMap(final K[] keys, final V[] values) {
    this(new HashIndexedArray<>(keys), values);
  }

  public HashIndexedArrayMap(final HashIndexedArray<K> keyIndex, final V[] values) {
    if (keyIndex.size() != values.length) {
      throw new IllegalArgumentException();
    }
    this.keyIndex = keyIndex;
    this.values = values;
  }

  public boolean isEmpty() {
    return values.length == 0;
  }

  public boolean isNotEmpty() {
    return values.length != 0;
  }

  public int size() {
    return values.length;
  }

  public HashIndexedArray<K> getKeyIndex() {
    return keyIndex;
  }

  public K[] keySet() {
    return keyIndex.keySet();
  }

  public V[] values() {
    return values;
  }

  public boolean containsKey(final K key) {
    return keyIndex.contains(key);
  }

  public V get(final K key) {
    final int index = keyIndex.getIndex(key);
    return index < 0 ? null : get(index);
  }

  public V get(final int keyIndex) {
    return values[keyIndex];
  }

  public V get(final K key, final V defaultValue) {
    final int index = keyIndex.getIndex(key);
    return index < 0 ? defaultValue : get(index, defaultValue);
  }

  public V get(final int keyIndex, final V defaultValue) {
    final V value = values[keyIndex];
    return value != null ? value : defaultValue;
  }

  public void clear() {
    Arrays.fill(values, null);
  }

  public void put(final int keyIndex, final V value) {
    values[keyIndex] = value;
  }

  public void copyToMap(final Map<K, V> map) {
    for (int i = 0; i < values.length; ++i) {
      map.put(keyIndex.get(i), values[i]);
    }
  }

  @SuppressWarnings("unchecked")
  public static <K, V> HashIndexedArrayMap<K, V> fromEntity(final V[] entities, final Function<V, K> keySupplier) {
    final Object[] keys = new Object[entities.length];
    for (int i = 0; i < entities.length; ++i) {
      keys[i] = keySupplier.apply(entities[i]);
    }
    return new HashIndexedArrayMap<>((K[])keys, entities);
  }

  @Override
  public String toString() {
    if (values == null || values.length == 0) {
      return "{}";
    }

    final StringBuilder dict = new StringBuilder();
    dict.append("{");
    for (int i = 0; i < values.length; ++i) {
      if (i > 0) dict.append(", ");
      dict.append(keyIndex.get(i)).append(":").append(values[i]);
    }
    dict.append("}");
    return dict.toString();
  }
}
