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

public class StringObjectMap extends HashMap<String, Object> {
  private static final long serialVersionUID = -5596935137960543036L;

  private static final StringObjectMap EMPTY_MAP = new StringObjectMap(Collections.emptyMap());

  public StringObjectMap() {
    super();
  }

  public StringObjectMap(final int initialCapacity) {
    super(initialCapacity);
  }

  public StringObjectMap(final int initialCapacity, final float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  public StringObjectMap(final Map<String, String> properties) {
    super(properties);
  }

  public StringObjectMap(final List<Entry<String, Object>> entries) {
    super(entries.size());
    for (final Entry<String, Object> entry: entries) {
      put(entry.getKey(), entry.getValue());
    }
  }

  public static StringObjectMap emptyMap() {
    return EMPTY_MAP;
  }

  public static StringMap singletonMap(final String key, final String value) {
    return new StringMap(Collections.singletonMap(key, value));
  }

  public static StringObjectMap fromKeyValues(final String[] key, final Object[] value) {
    final StringObjectMap map = new StringObjectMap(key.length);
    for (int i = 0; i < key.length; ++i) {
      map.put(key[i], value[i]);
    }
    return map;
  }

  public void removeAll(final Collection<String> keys) {
    for (final String key: keys) {
      this.remove(key);
    }
  }

  // --------------------------------------------------------------------------------
  // Property helpers
  // --------------------------------------------------------------------------------
  public String getString(final String key, final String defaultValue) {
    final String value = (String) this.get(key);
    return value != null ? value : defaultValue;
  }

  public int getInt(final String key, final int defaultValue) {
    final Number value = (Number) this.get(key);
    return value != null ? value.intValue() : defaultValue;
  }

  public long getLong(final String key, final long defaultValue) {
    final Number value = (Number) this.get(key);
    return value != null ? value.longValue() : defaultValue;
  }

  public float getFloat(final String key, final float defaultValue) {
    final Number value = (Number) this.get(key);
    return value != null ? value.floatValue() : defaultValue;
  }

  public double getDouble(final String key, final double defaultValue) {
    final Number value = (Number) this.get(key);
    return value != null ? value.doubleValue() : defaultValue;
  }

  public boolean getBoolean(final String key, final boolean defaultValue) {
    final Boolean value = (Boolean) this.get(key);
    return value != null ? value.booleanValue() : defaultValue;
  }

  @SuppressWarnings("unchecked")
  public <T extends Enum<T>> T getEnumValue(final String key, final T defaultValue) {
    final T value = (T) this.get(key);
    return value != null ? value : defaultValue;
  }

  // --------------------------------------------------------------------------------
  // List lookup helpers
  // --------------------------------------------------------------------------------
  public String[] getStringList(final String key, final String[] defaultValue) {
    final String[] value = (String[]) this.get(key);
    return value != null ? value : defaultValue;
  }

  public int[] getIntList(final String key, final int[] defaultValue) {
    final int[] value = (int[]) this.get(key);
    return value != null ? value : defaultValue;
  }
}
