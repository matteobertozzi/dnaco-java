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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringMap extends HashMap<String, String> {
  private static final long serialVersionUID = 8330625977658465233L;

  private static final StringMap EMPTY_MAP = new StringMap(Collections.emptyMap());

  public StringMap() {
    super();
  }

  public StringMap(final int initialCapacity) {
    super(initialCapacity);
  }

  public StringMap(final int initialCapacity, final float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  public StringMap(final Map<String, String> properties) {
    super(properties);
  }

  public StringMap(final List<Entry<String, String>> entries) {
    super(entries.size());
    for (final Entry<String, String> entry: entries) {
      put(entry.getKey(), entry.getValue());
    }
  }

  public static StringMap emptyMap() {
    return EMPTY_MAP;
  }

  public static StringMap singletonMap(final String key, final String value) {
    return new StringMap(Collections.singletonMap(key, value));
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
