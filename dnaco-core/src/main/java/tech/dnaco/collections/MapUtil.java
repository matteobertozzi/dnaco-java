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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public final class MapUtil {
  private MapUtil() {
    // no-op
  }

  public static <K, V> Map<K, V> emptyIfNull(final Map<K, V> input) {
    return input == null ? Collections.emptyMap() : input;
  }

  public static <K, V> boolean isEmpty(final Map<K, V> input) {
    return input == null || input.isEmpty();
  }

  public static <K, V> boolean isNotEmpty(final Map<K, V> input) {
    return input != null && !input.isEmpty();
  }

  public static <K, V> int size(final Map<K, V> input) {
    return input != null ? input.size() : 0;
  }

  public static <K, V> V upsert(final Map<K, V> map, final K key, final V newValue) {
    map.put(key, newValue);
    return newValue;
  }

  public static <K, V> void removeAll(final Map<K, V> map, final Collection<K> keys) {
    for (K key: keys) {
      map.remove(key);
    }
  }

  // ================================================================================
  //  Multi-Map helpers
  // ================================================================================
  public static <K, V> List<V> addToList(final Map<K, List<V>> map, final K key, final V newValue) {
    final List<V> values = map.get(key);
    if (values != null) {
      values.add(newValue);
      return values;
    }

    final List<V> vList = new ArrayList<>();
    vList.add(newValue);
    map.put(key, vList);
    return vList;
  }

  public static <K, V> Set<V> addToSet(final Map<K, Set<V>> map, final K key, final V newValue) {
    final Set<V> values = map.get(key);
    if (values != null) {
      values.add(newValue);
      return values;
    }

    final Set<V> vList = new HashSet<>();
    vList.add(newValue);
    map.put(key, vList);
    return vList;
  }

  public static <K, V extends Comparable<V>> Set<V> addToOrderedSet(final Map<K, Set<V>> map, final K key, final V newValue) {
    final Set<V> values = map.get(key);
    if (values != null) {
      values.add(newValue);
      return values;
    }

    final Set<V> vList = new TreeSet<>();
    vList.add(newValue);
    map.put(key, vList);
    return vList;
  }

  public static <K, V> Set<V> removeFromSet(final Map<K, Set<V>> map, final K key, final V valueToRemove) {
    final Set<V> values = map.get(key);
    if (values == null) return null;

    values.remove(valueToRemove);
    return values;
  }
}
