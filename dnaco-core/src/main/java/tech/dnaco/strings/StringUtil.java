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

import java.util.ArrayList;
import java.util.Comparator;

public final class StringUtil {
  public static final char[] ALPHA_NUMERIC_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
  public static final char[] ASCII_UPPERCASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
  public static final char[] ASCII_LOWERCASE = "abcdefghijklmnopqrstuvwxyz".toCharArray();
  public static final char[] HEX_DIGITS = "0123456789abcdefABCDEF".toCharArray();

  public static final String[] EMPTY_ARRAY = new String[0];

  private StringUtil() {
    // no-op
  }

  // ================================================================================
  //  String length related
  // ================================================================================
  public static int length(final String input) {
    return input == null ? 0 : input.length();
  }

  public static boolean isEmpty(final String input) {
    return (input == null) || (input.length() == 0);
  }

  public static boolean isNotEmpty(final String input) {
    return (input != null) && input.length() > 0;
  }

  // ================================================================================
  //  String value related
  // ================================================================================
  public static String emptyIfNull(final String input) {
    return input == null ? "" : input;
  }

  public static String nullIfEmpty(final String input) {
    return isEmpty(input) ? null : input;
  }

  public static String defaultIfEmpty(final String input, final String defaultValue) {
    return isNotEmpty(input) ? input : defaultValue;
  }

  // ================================================================================
  //  String upper/lower case related
  // ================================================================================
  public static String toUpper(final String value) {
    return value != null ? value.toUpperCase() : null;
  }

  public static String toLower(final String value) {
    return value != null ? value.toLowerCase() : null;
  }

  // ================================================================================
  //  String trim related
  // ================================================================================
  public static String trim(final String input) {
    return isEmpty(input) ? input : input.trim();
  }

  public static String trimToEmpty(final String input) {
    return isEmpty(input) ? "" : input.trim();
  }

  public static String trimToNull(final String input) {
    return isEmpty(input) ? null : nullIfEmpty(input.trim());
  }

  public static String ltrim(final String input) {
    if (input == null) return null;
    final int length = input.length();
    int st = 0;
    while (st < length && Character.isWhitespace(input.charAt(st))) {
      st++;
    }
    return st > 0 ? input.substring(st) : input;
  }

  public static String rtrim(final String input) {
    if (input == null) return null;
    int length = input.length();
    while (length > 0 && Character.isWhitespace(input.charAt(length - 1))) {
      length--;
    }
    return length != input.length() ? input.substring(0, length) : input;
  }

  public static String collapseSpaces(final String input) {
    return isEmpty(input) ? input : input.replaceAll("\\s+", " ");
  }

  public static String[] splitAndTrim(final String input, final String delimiter) {
    final String itrim = input != null ? input.trim() : null;
    if (isEmpty(itrim)) return null;

    final String[] items = itrim.split(delimiter);
    for (int i = 0; i < items.length; ++i) {
      items[i] = items[i].trim();
    }
    return items;
  }

  public static String[] splitAndTrimSkipEmptyLines(final String input, final String delimiter) {
    final String itrim = input != null ? input.trim() : null;
    if (isEmpty(itrim)) return null;

    final String[] rawItems = itrim.split(delimiter);
    final ArrayList<String> items = new ArrayList<>(rawItems.length);
    for (int i = 0; i < rawItems.length; ++i) {
      final String row = StringUtil.trim(rawItems[i]);
      if (StringUtil.isNotEmpty(row)) {
        items.add(row);
      }
    }
    return items.toArray(new String[0]);
  }

  // ================================================================================
  //  StringBuilder helpers
  // ================================================================================
  public static StringBuilder append(final StringBuilder sb, final char v, long count) {
    for (; count > 0; --count) sb.append(v);
    return sb;
  }

  public static StringBuilder append(final StringBuilder sb, final String v, long count) {
    for (; count > 0; --count) sb.append(v);
    return sb;
  }

  // ================================================================================
  //  String comparison related
  // ================================================================================
  @SuppressWarnings({ "StringEquality", "EqualsReplaceableByObjectsCall" })
  public static boolean equals(final String a, final String b) {
    return (a == b) || (a != null && a.equals(b));
  }

  @SuppressWarnings("StringEquality")
  public static boolean equalsIgnoreCase(final String a, final String b) {
    return (a == b) || (a != null && a.equalsIgnoreCase(b));
  }

  @SuppressWarnings("StringEquality")
  public static int compareTo(final String a, final String b) {
    if (a == b) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    return a.compareTo(b);
  }

  @SuppressWarnings("StringEquality")
  public static int compareToIgnoreCase(final String a, final String b) {
    if (a == b) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    return a.compareToIgnoreCase(b);
  }

  public static boolean equals(final String a, final int aOff, final int aLen,
      final String b, final int bOff, final int bLen) {
    if (aLen != bLen) return false;

    for (int i = 0; i < aLen; ++i) {
      if (a.charAt(aOff + i) != b.charAt(bOff + i)) {
        return false;
      }
    }
    return true;
  }

  public static final Comparator<String> STRING_REVERSE_COMPARATOR = (a, b) -> StringUtil.compareTo(b, a);
}
