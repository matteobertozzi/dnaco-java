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

import java.sql.Savepoint;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import tech.dnaco.strings.StringConverter;

public class StringObjectMap extends OrderedHashMap<String, Object> {
  public StringObjectMap() {
    super(0);
  }

  public StringObjectMap(final int initialSize) {
    super(initialSize);
  }

  public StringObjectMap(final Map<String, Object> properties) {
    this(properties.entrySet());
  }

  public StringObjectMap(final Collection<Entry<String, Object>> entries) {
    super(entries.size());
    for (final Entry<String, Object> entry: entries) {
      put(entry.getKey(), entry.getValue());
    }
  }

  public static StringObjectMap singletonMap(final String key, final Object value) {
    return new StringObjectMap(Map.of(key, value));
  }

  public static StringObjectMap fromKeyValues(final String[] key, final Object[] value) {
    final StringObjectMap map = new StringObjectMap(key.length);
    for (int i = 0; i < key.length; ++i) {
      map.put(key[i], value[i]);
    }
    return map;
  }

  @Override
  protected boolean keyEquals(final Object a, final Object b) {
    return Objects.equals(a, b);
  }

  // ================================================================================
  // Property helpers
  // ================================================================================
  public String getString(final String key, final String defaultValue) {
    final String value = (String) this.get(key);
    return value != null ? value : defaultValue;
  }

  public int getInt(final String key, final int defaultValue) {
    final Object oValue = this.get(key);
    if (oValue == null) return defaultValue;

    if (oValue instanceof final Number value) {
      return value.intValue();
    } else if (oValue instanceof final String sValue) {
      return StringConverter.toInt(sValue, defaultValue);
    }
    throw new IllegalArgumentException("expected a int got: " + oValue.getClass() + " " + oValue);
  }

  public long getLong(final String key, final long defaultValue) {
    final Object oValue = this.get(key);
    if (oValue == null) return defaultValue;

    if (oValue instanceof final Number value) {
      return value.longValue();
    } else if (oValue instanceof final String sValue) {
      return StringConverter.toLong(sValue, defaultValue);
    }
    throw new IllegalArgumentException("expected a long got: " + oValue.getClass() + " " + oValue);
  }

  public float getFloat(final String key, final float defaultValue) {
    final Object oValue = this.get(key);
    if (oValue == null) return defaultValue;

    if (oValue instanceof final Number value) {
      return value.floatValue();
    } else if (oValue instanceof final String sValue) {
      return StringConverter.toFloat(sValue, defaultValue);
    }
    throw new IllegalArgumentException("expected a float got: " + oValue.getClass() + " " + oValue);
  }

  public double getDouble(final String key, final double defaultValue) {
    final Object oValue = this.get(key);
    if (oValue == null) return defaultValue;

    if (oValue instanceof final Number value) {
      return value.floatValue();
    } else if (oValue instanceof final String sValue) {
      return StringConverter.toDouble(sValue, defaultValue);
    }
    throw new IllegalArgumentException("expected a double got: " + oValue.getClass() + " " + oValue);
  }

  public boolean getBoolean(final String key, final boolean defaultValue) {
    final Object oValue = this.get(key);
    if (oValue == null) return defaultValue;

    if (oValue instanceof final Boolean value) {
      return value;
    } else if (oValue instanceof final String sValue) {
      return StringConverter.toBoolean(sValue, defaultValue);
    }
    throw new IllegalArgumentException("expected a boolean got: " + oValue.getClass() + " " + oValue);
  }

  @SuppressWarnings("unchecked")
  public <T extends Enum<T>> T getEnumValue(final String key, final T defaultValue) {
    final T value = (T) this.get(key);
    return value != null ? value : defaultValue;
  }

  // ================================================================================
  // List lookup helpers
  // ================================================================================
  public String[] getStringList(final String key, final String[] defaultValue) {
    final String[] value = (String[]) this.get(key);
    return value != null ? value : defaultValue;
  }

  public int[] getIntList(final String key, final int[] defaultValue) {
    final int[] value = (int[]) this.get(key);
    return value != null ? value : defaultValue;
  }
}
