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

import java.util.Collection;
import java.util.Map;

import tech.dnaco.strings.StringConverter;
import tech.dnaco.strings.StringUtil;

public class StringMap extends OrderedHashMap<String, String> {
  private final boolean caseSensitive;

  public StringMap() {
    this(0, true);
  }

  public StringMap(final int initialSize) {
    this(initialSize, true);
  }

  public StringMap(final int initialSize, final boolean caseSensitive) {
    super(initialSize);
    this.caseSensitive = caseSensitive;
  }

  public StringMap(final Map<String, String> properties) {
    this(properties.entrySet());
  }

  public StringMap(final Collection<Entry<String, String>> entries) {
    this(entries, true);
  }

  public StringMap(final Collection<Entry<String, String>> entries, final boolean caseSensitive) {
    super(entries.size());
    for (final Entry<String, String> entry: entries) {
      put(entry.getKey(), entry.getValue());
    }
    this.caseSensitive = caseSensitive;
  }

  public static StringMap singletonMap(final String key, final String value) {
    return new StringMap(Map.of(key, value));
  }

  public static StringMap fromKeyValues(final String[] key, final String[] value) {
    final StringMap map = new StringMap(key.length);
    for (int i = 0; i < key.length; ++i) {
      map.put(key[i], value[i]);
    }
    return map;
  }

  public static StringMap caseInsensitiveMap() {
    return new StringMap(8, false);
  }

  public static StringMap caseInsensitiveMap(final Map<String, String> properties) {
    return caseInsensitiveMap(properties.entrySet());
  }

  public static StringMap caseInsensitiveMap(final Collection<Entry<String, String>> entries) {
    return new StringMap(entries, false);
  }

  @Override
  protected boolean keyEquals(final Object a, final Object b) {
    return caseSensitive ? StringUtil.equals((String)a, (String)b) : StringUtil.equalsIgnoreCase((String)a, (String)b);
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
}
