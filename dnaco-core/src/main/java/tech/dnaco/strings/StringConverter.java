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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import tech.dnaco.logging.Logger;

public final class StringConverter {
  private static final String DEFAULT_KEY = "value";

  private StringConverter() {
    // no-op
  }

  public static int toInt(final String inputValue, final int defaultValue) {
    return toInt(DEFAULT_KEY, inputValue, defaultValue);
  }

  public static int toInt(final String key, final String inputValue, final int defaultValue) {
    final String value = StringUtil.trim(inputValue);
    if (!StringUtil.isEmpty(value)) {
      try {
        return Integer.parseInt(value);
      } catch (final Exception e) {
        Logger.warn("unable to parse {}: {}", key, value);
      }
    }
    return defaultValue;
  }

  public static Integer tryToInt(final String inputValue) {
    final String value = StringUtil.trim(inputValue);
    if (StringUtil.isEmpty(value)) return null;
    try {
      return Integer.parseInt(value);
    } catch (final Exception e) {
      return null;
    }
  }

  public static long toLong(final String inputValue, final long defaultValue) {
    return toLong(DEFAULT_KEY, inputValue, defaultValue);
  }

  public static long toLong(final String key, final String inputValue, final long defaultValue) {
    final String value = StringUtil.trim(inputValue);
    if (!StringUtil.isEmpty(value)) {
      try {
        return Long.parseLong(value);
      } catch (final Exception e) {
        Logger.warn("unable to parse {}: {}", key, value);
      }
    }
    return defaultValue;
  }

  public static Long tryToLong(final String inputValue) {
    final String value = StringUtil.trim(inputValue);
    if (StringUtil.isEmpty(value)) return null;
    try {
      return Long.parseLong(value);
    } catch (final Exception e) {
      return null;
    }
  }

  public static short toShort(final String inputValue, final short defaultValue) {
    return toShort(DEFAULT_KEY, inputValue, defaultValue);
  }

  public static short toShort(final String key, final String inputValue, final short defaultValue) {
    final String value = StringUtil.trim(inputValue);
    if (!StringUtil.isEmpty(value)) {
      try {
        return Short.parseShort(value);
      } catch (final Exception e) {
        Logger.warn("unable to parse {}: {}", key, value);
      }
    }
    return defaultValue;
  }

  public static Short tryToShort(final String inputValue) {
    final String value = StringUtil.trim(inputValue);
    if (StringUtil.isEmpty(value)) return null;
    try {
      return Short.parseShort(value);
    } catch (final Exception e) {
      return null;
    }
  }

  public static float toFloat(final String inputValue, final float defaultValue) {
    return toFloat(DEFAULT_KEY, inputValue, defaultValue);
  }

  public static float toFloat(final String key, final String inputValue, final float defaultValue) {
    final String value = StringUtil.trim(inputValue);
    if (!StringUtil.isEmpty(value)) {
      try {
        return Float.parseFloat(value);
      } catch (final Exception e) {
        try {
          return Float.parseFloat(value.replace(',', '.'));
        } catch (final Exception e2) {
          Logger.warn("unable to parse {}: {}", key, value);
        }
      }
    }
    return defaultValue;
  }

  public static Float tryToFloat(final String inputValue) {
    final String value = StringUtil.trim(inputValue);
    if (StringUtil.isEmpty(value)) return null;
    try {
      return Float.parseFloat(value);
    } catch (final Exception e) {
      try {
        return Float.parseFloat(value.replace(',', '.'));
      } catch (final Exception e2) {
        return null;
      }
    }
  }

  public static double toDouble(final String inputValue, final double defaultValue) {
    return toDouble(DEFAULT_KEY, inputValue, defaultValue);
  }

  public static double toDouble(final String key, final String inputValue, final double defaultValue) {
    final String value = StringUtil.trim(inputValue);
    if (!StringUtil.isEmpty(value)) {
      try {
        return Double.parseDouble(value);
      } catch (final Exception e) {
        try {
          return Double.parseDouble(value.replace(',', '.'));
        } catch (final Exception e2) {
          Logger.warn("unable to parse {}: {}", key, value);
        }
      }
    }
    return defaultValue;
  }

  public static Double tryToDouble(final String inputValue) {
    final String value = StringUtil.trim(inputValue);
    if (StringUtil.isEmpty(value)) return null;
    try {
      return Double.parseDouble(value);
    } catch (final Exception e) {
      try {
        return Double.parseDouble(value.replace(',', '.'));
      } catch (final Exception e2) {
        return null;
      }
    }
  }

  public static BigDecimal toDecimal(final String inputValue, final BigDecimal defaultValue) {
    return toDecimal(DEFAULT_KEY, inputValue, defaultValue);
  }

  public static BigDecimal toDecimal(final String key, final String inputValue, final BigDecimal defaultValue) {
    final String value = StringUtil.trim(inputValue);
    if (!StringUtil.isEmpty(value)) {
      try {
        return new BigDecimal(value);
      } catch (final Exception e) {
        try {
          return new BigDecimal(value.replace(',', '.'));
        } catch (final Exception e2) {
          Logger.warn("unable to parse {}: {}", key, value);
        }
      }
    }
    return defaultValue;
  }

  public static BigDecimal tryToDecimal(final String inputValue) {
    final String value = StringUtil.trim(inputValue);
    if (StringUtil.isEmpty(value)) return null;
    try {
      return new BigDecimal(value);
    } catch (final Exception e) {
      try {
        return new BigDecimal(value.replace(',', '.'));
      } catch (final Exception e2) {
        return null;
      }
    }
  }

  public static boolean toBoolean(final String inputValue, final boolean defaultValue) {
    return toBoolean(DEFAULT_KEY, inputValue, defaultValue);
  }

  public static boolean toBoolean(final String key, final String inputValue, final boolean defaultValue) {
    final String value = StringUtil.trim(inputValue);
    if (StringUtil.isNotEmpty(value)) {
      switch (value.toLowerCase()) {
        case "1":
        case "yes":
        case "true":
          return true;
        case "false":
        case "no":
        case "0":
          return false;
        default:
          Logger.warn("unable to parse {}: {}", key, value);
      }
    }
    return defaultValue;
  }


  public static Boolean tryToBoolean(final String inputValue) {
    final String value = StringUtil.trim(inputValue);
    if (StringUtil.isNotEmpty(value)) {
      switch (value.toLowerCase()) {
        case "1":
        case "yes":
        case "true":
          return true;
        case "false":
        case "no":
        case "0":
          return false;
      }
    }
    return null;
  }

  public static <T extends Enum<T>> T toEnumValue(final Class<T> enumType, final String inputValue, final T defaultValue) {
    return toEnumValue(enumType, DEFAULT_KEY, inputValue, defaultValue);
  }

  public static <T extends Enum<T>> T toEnumValue(final Class<T> enumType, final String key, final String inputValue,
      final T defaultValue) {
    final String value = StringUtil.trim(inputValue);
    if (!StringUtil.isEmpty(value)) {
      try {
        return Enum.valueOf(enumType, value);
      } catch (final Exception e) {
        Logger.warn("unable to parse {}: {}", key, value);
      }
    }
    return defaultValue;
  }

  // --------------------------------------------------------------------------------
  // List lookup helpers
  // --------------------------------------------------------------------------------
  public static String[] toStringList(final String inputValue, final String[] defaultValue) {
    return toStringList(DEFAULT_KEY, inputValue, defaultValue);
  }

  public static String[] toStringList(final String key, final String inputValue, final String[] defaultValue) {
    return toStringList(key, ",", inputValue, defaultValue);
  }

  public static String[] toStringList(final String key, final String separator, final String inputValue, final String[] defaultValue) {
    return inputValue != null ? StringUtil.splitAndTrim(inputValue, separator) : defaultValue;
  }

  public static int[] toIntList(final String inputValue, final int[] defaultValue) {
    return toIntList(DEFAULT_KEY, inputValue, defaultValue);
  }

  public static int[] toIntList(final String key, final String inputValue, final int[] defaultValue) {
    return toIntList(key, ",", inputValue, defaultValue);
  }

  public static int[] toIntList(final String key, final String separator, final String inputValue, final int[] defaultValue) {
    final String[] value = toStringList(key, separator, inputValue, null);
    if (value == null) return defaultValue;

    final int[] items = new int[value.length];
    try {
      for (int i = 0; i < value.length; ++i) {
        items[i] = Integer.parseInt(value[i]);
      }
      return items;
    } catch (final Exception e) {
      Logger.warn("unable to parse {}: {}", key, Arrays.toString(value));
    }
    return defaultValue;
  }

  public static int[] toIntList(final List<String> values) {
    if (values == null) return null;

    final int[] intValues = new int[values.size()];
    int index = 0;
    for (final String v: values) {
      intValues[index++] = toInt(v, 0);
    }
    return intValues;
  }
}

