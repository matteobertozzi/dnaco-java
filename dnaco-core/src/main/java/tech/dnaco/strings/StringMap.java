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

package tech.dnaco.strings;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class StringMap implements Map<String, String> {
  private static final StringMap EMPTY_MAP = new StringMap(Collections.emptyMap());

  private transient final Map<String, String> map;

  public StringMap() {
    this(new HashMap<>());
  }

  public StringMap(final int initialCapacity) {
    this(new HashMap<>(initialCapacity));
  }

  public StringMap(final int initialCapacity, final float loadFactor) {
    this(new HashMap<>(initialCapacity, loadFactor));
  }

  private static StringMap emptyMap() {
    return EMPTY_MAP;
  }

  public static StringMap caseInsensitiveMap(final String key, final String value) {
    return caseInsensitiveMap(Collections.singletonMap(key, value));
  }

  public static StringMap caseInsensitiveMap() {
    return new StringMap(new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
  }

  public static StringMap caseInsensitiveMap(final Map<String, String> properties) {
    final StringMap map = new StringMap(new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
    map.putAll(properties);
    return map;
  }

  public static StringMap caseInsensitiveMap(final List<Entry<String, String>> entries) {
    final TreeMap<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (Entry<String, String> entry: entries) {
      map.put(entry.getKey(), entry.getValue());
    }
    return new StringMap(map);
  }

  protected StringMap(final Map<String, String> properties) {
    this.map = properties;
  }

  // --------------------------------------------------------------------------------
  //  Map methods
  // --------------------------------------------------------------------------------
  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public String get(Object key) {
    return map.get(key);
  }

  @Override
  public String put(String key, String value) {
    return map.put(key, value);
  }

  @Override
  public String remove(Object key) {
    return map.remove(key);
  }

  @Override
  public void putAll(Map<? extends String, ? extends String> m) {
    map.putAll(m);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Set<String> keySet() {
    return map.keySet();
  }

  @Override
  public Collection<String> values() {
    return map.values();
  }

  @Override
  public Set<Entry<String, String>> entrySet() {
    return map.entrySet();
  }

  // --------------------------------------------------------------------------------
  // Property helpers
  // --------------------------------------------------------------------------------
  public String getString(final String key, final String defaultValue) {
    final String value = this.get(key);
    return value != null ? value : defaultValue;
  }

  public int getInt(final String key, final int defaultValue) {
    return StringConverter.toInt(key, this.get(key), defaultValue);
  }

  public long getLong(final String key, final long defaultValue) {
    return StringConverter.toLong(key, this.get(key), defaultValue);
  }

  public float getFloat(final String key, final float defaultValue) {
    return StringConverter.toFloat(key, this.get(key), defaultValue);
  }

  public double getDouble(final String key, final double defaultValue) {
    return StringConverter.toDouble(key, this.get(key), defaultValue);
  }

  public boolean getBoolean(final String key, final boolean defaultValue) {
    return StringConverter.toBoolean(key, this.get(key), defaultValue);
  }

  public <T extends Enum<T>> T getEnumValue(final Class<T> enumType, final String key, final T defaultValue) {
    return StringConverter.toEnumValue(enumType, key, this.get(key), defaultValue);
  }

  // --------------------------------------------------------------------------------
  // List lookup helpers
  // --------------------------------------------------------------------------------
  public String[] getStringList(final String key, final String[] defaultValue) {
    return StringConverter.toStringList(key, this.get(key), defaultValue);
  }

  public int[] getIntList(final String key, final int[] defaultValue) {
    return StringConverter.toIntList(key, this.get(key), defaultValue);
  }

  @Override
  public String toString() {
    return map.toString();
  }
}
