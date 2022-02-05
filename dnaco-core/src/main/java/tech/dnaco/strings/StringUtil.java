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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.regex.Pattern;

import tech.dnaco.collections.maps.StringMap;

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
  //  String contains related
  // ================================================================================
  public static boolean contains(final String text, final String pattern) {
    return StringUtil.isNotEmpty(text) && text.contains(pattern);
  }

  public static boolean startsWith(final String text, final String prefix) {
    return StringUtil.isNotEmpty(text) && text.startsWith(prefix);
  }

  // ================================================================================
  //  String upper/lower/capitalize case related
  // ================================================================================
  public static String toUpper(final String value) {
    return value != null ? value.toUpperCase() : null;
  }

  public static String toLower(final String value) {
    return value != null ? value.toLowerCase() : null;
  }

  public static String capitalize(final String value) {
    return capitalize(value, true);
  }

  public static String capitalize(final String value, final boolean everythingLower) {
    if (StringUtil.isEmpty(value)) return value;

    final StringBuilder sb = new StringBuilder(value.length());
    sb.append(Character.toUpperCase(value.charAt(0)));
    for (int i = 1; i < value.length(); ++i) {
      if (everythingLower) {
        sb.append(Character.toLowerCase(value.charAt(i)));
      } else {
        sb.append(value.charAt(i));
      }
    }
    return sb.toString();
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
  //  String cut/substring helpers
  // ================================================================================
  public static String cut(final String text, final int length) {
    if (StringUtil.isEmpty(text)) return text;
    return text.length() > length ? text.substring(0, length) : text;
  }

  public static String substring(final String text, final int beginIndex, final int endIndex) {
    if (StringUtil.isEmpty(text)) return text;
    if (beginIndex >= text.length()) return "";
    return endIndex < 0 ? text.substring(beginIndex) : text.substring(beginIndex, Math.min(text.length(), endIndex));
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
  // Join helpers
  // ================================================================================
  public static String join(final String delimiter, final String... items) {
    return join(delimiter, items, 0, items.length);
  }

  public static String join(final String delimiter, final String[] items, final int offset, final int count) {
    if (count == 0) return "";

    int size = 0;
    for (int i = 0; i < count; ++i) {
      if (items[offset + i] != null) {
        if (size > 0) size += delimiter.length();
        size += items[offset + i].length();
      }
    }

    final StringBuilder builder = new StringBuilder(size);
    for (int i = 0; i < count; ++i) {
      if (items[offset + i] != null) {
        if (builder.length() > 0) builder.append(delimiter);
        builder.append(items[offset + i]);
      }
    }
    return builder.toString();
  }

  public static <T> String join(final String delimiter, final Collection<T> items) {
    if (items.isEmpty()) return "";

    int index = 0;
    final StringBuilder builder = new StringBuilder();
    for (final T item: items) {
      if (item == null) continue;

      if (index++ > 0) builder.append(delimiter);
      builder.append(item);
    }
    return builder.toString();
  }

  public static <T> String join(final String delimiter, final Collection<T> items, final boolean preAndPostDelimiter) {
    if (items.isEmpty()) return "";

    int index = 0;
    final StringBuilder builder = new StringBuilder();
    if (preAndPostDelimiter) builder.append(delimiter);
    for (final T item: items) {
      if (item == null) continue;

      if (index++ > 0) builder.append(delimiter);
      builder.append(item);
    }
    if (preAndPostDelimiter) builder.append(delimiter);
    return builder.toString();
  }

  public static String join(final String delimiter, final int count, final String symbol) {
    if (count == 0) return "";

    final StringBuilder builder = new StringBuilder(((count - 1) * delimiter.length()) + (count * symbol.length()));
    builder.append(symbol);
    for (int i = 1; i < count; ++i) {
      builder.append(delimiter);
      builder.append(symbol);
    }
    return builder.toString();
  }

  public static String join(final String delimiter, final StringMap dataMap, final String... keys) {
    if (keys.length == 0) return "";

    final StringBuilder builder = new StringBuilder(((keys.length - 1) * delimiter.length()));
    builder.append(dataMap.get(keys[0]));
    for (int i = 1; i < keys.length; ++i) {
      builder.append(delimiter);
      builder.append(keys[i]);
    }
    return builder.toString();
  }

  // ================================================================================
  //  Text Replace related - TODO: IMPROVE-ME
  // ================================================================================
  public static String replace(final String s, final String repl, final String replWith) {
    if (StringUtil.isEmpty(s)) return s;

    int index = s.indexOf(repl);
    if (index < 0) return s;

    int prevIndex = 0;
    final StringBuilder builder = new StringBuilder(s.length() - repl.length() + replWith.length());
    while (index >= 0) {
      builder.append(s, prevIndex, index);
      builder.append(replWith);

      prevIndex = index + repl.length();
      index = s.indexOf(repl, prevIndex);
    }
    builder.append(s, prevIndex, s.length());
    return builder.toString();
  }

  public static String replace(final String s, final char repl, final String replWith) {
    if (StringUtil.isEmpty(s)) return s;

    int index = s.indexOf(repl);
    if (index < 0) return s;

    int prevIndex = 0;
    final StringBuilder builder = new StringBuilder(s.length() - 1 + replWith.length());
    while (index >= 0) {
      builder.append(s, prevIndex, index);
      builder.append(replWith);

      prevIndex = index + 1;
      index = s.indexOf(repl, prevIndex);
    }
    builder.append(s, prevIndex, s.length());
    return builder.toString();
  }

  public static String replace(final String s, final String repl, final char replWith) {
    if (StringUtil.isEmpty(s)) return s;

    int index = s.indexOf(repl);
    if (index < 0) return s;

    int prevIndex = 0;
    final StringBuilder builder = new StringBuilder(s.length() - repl.length());
    while (index >= 0) {
      builder.append(s, prevIndex, index);
      builder.append(replWith);

      prevIndex = index + repl.length();
      index = s.indexOf(repl, prevIndex);
    }
    builder.append(s, prevIndex, s.length());
    return builder.toString();
  }

  // ================================================================================
  //  Text Lines - TODO: IMPROVE-ME
  // ================================================================================
  public static void foreachLine(final String text, final LineConsumer consumer) {
    int offset = 0;
    int lineNumber = 1;
    while (offset < text.length()) {
      final int newOffset = text.indexOf('\n', offset);
      if (newOffset < 0) break;

      consumer.consume(text, lineNumber, offset, newOffset - offset);
      offset = newOffset + 1;
      lineNumber++;
    }

    if (offset < text.length()) {
      consumer.consume(text, lineNumber, offset, text.length() - offset);
    }
  }

  @FunctionalInterface
  public interface LineConsumer {
    void consume(String text, int lineNumber, int offset, int length);
  }

  // ================================================================================
  //  Pad helpers
  // ================================================================================
  public static String fill(final char ch, final int length) {
    final char[] pad = new char[length];
    Arrays.fill(pad, ch);
    return new String(pad);
  }

  public static String padLeft(final int data, final char padCh, final int length) throws LimitExceededException {
    return padLeft(Integer.toString(data), padCh, length);
  }

  public static String padLeft(final String data, final char padCh, final int length) throws LimitExceededException {
    return pad(data, padCh, length, true);
  }

  public static String padRight(final int data, final char padCh, final int length) throws LimitExceededException {
    return padRight(Integer.toString(data), padCh, length);
  }

  public static String padRight(final String data, final char padCh, final int length) throws LimitExceededException {
    return pad(data, padCh, length, false);
  }

  private static String pad(final String data, final char padCh, final int length, final boolean padLeft) throws LimitExceededException {
    if (StringUtil.isEmpty(data)) {
      return fill(padCh, length);
    }

    if (data.length() >= length) {
      if (data.length() > length) {
        throw new LimitExceededException(length, data);
      }
      return data;
    }

    final char[] pad = new char[length - data.length()];
    Arrays.fill(pad, padCh);

    final StringBuilder sb = new StringBuilder(length);
    if (padLeft) {
      sb.append(pad).append(data);
    } else {
      sb.append(data).append(pad);
    }
    return sb.toString();
  }

  public static final class LimitExceededException extends Exception {
    private static final long serialVersionUID = 5689070745285361428L;

    public LimitExceededException(final String msg) {
      super(msg);
    }

    public LimitExceededException(final int expectedLength, final String data) {
      super(String.format("limit exceeded: expected %d got %d: '%s'", expectedLength, data.length(), data));
    }
  }

  // ================================================================================
  //  String like related
  // ================================================================================
  public static boolean like(final String source, final String exp) {
    if (source == null || exp == null) {
      return false;
    }

    if (source.isEmpty() && exp.isEmpty()) {
      return true;
    }

    return likePattern(exp).matches(source);
  }

  public static LikePattern likePattern(final String patternString) {
    if (patternString == null) return LikeWithNoMatch.INSTANCE;
    if (patternString.isEmpty()) return new LikeWithStringEquals("");

    final char ESCAPE_CHAR = '\\';
    int anythingWildcards = 0;
    int singleWildcards = 0;

    final StringBuilder regex = new StringBuilder(patternString.length() * 2);
    regex.append('^');
    boolean escaped = false;
    for (int i = 0, n = patternString.length(); i < n; ++i) {
      final char currentChar = patternString.charAt(i);
      if (!(!escaped || currentChar == '%' || currentChar == '_' || currentChar == ESCAPE_CHAR)) {
        throw new IllegalArgumentException("Escape character must be followed by '%%', '_' or the escape character itself");
      }
      //if (shouldEscape && !escaped && (currentChar == escapeChar)) {
      if (!escaped && (currentChar == ESCAPE_CHAR)) {
        escaped = true;
      } else {
        switch (currentChar) {
          case '%':
            regex.append(escaped ? "%" : ".*");
            escaped = false;
            anythingWildcards++;
            break;
          case '_':
            regex.append(escaped ? '_' : '.');
            escaped = false;
            singleWildcards++;
            break;
          default:
            // escape special regex characters
            switch (currentChar) {
              case '\\':
              case '^':
              case '$':
              case '.':
              case '*':
                regex.append('\\');
                break;
            }
            regex.append(currentChar);
            escaped = false;
            break;
        }
      }
    }
    if (escaped) {
      throw new IllegalArgumentException("Escape character must be followed by '%%', '_' or the escape character itself");
    }
    regex.append('$');

    if (anythingWildcards == patternString.length()) {
      return LikeMatchingEverything.INSTANCE;
    } else if (anythingWildcards > 0 || singleWildcards > 0) {
      // if (singleWildcards == patternString.length()) return new LikeWithStringLengthEquals(singleWildCards);
      final char firstChar = patternString.charAt(0);
      final boolean prefixMatch = (firstChar != '%' && firstChar != '_');
      return new RegexLikePattern(Pattern.compile(regex.toString()), prefixMatch);
    }
    return new LikeWithStringEquals(patternString);
  }

  public static interface LikePattern {
    enum MatchType { NOTHING, EVERYTHING, PREFIX, FULL, RANDOM }
    boolean matches(final String input);
    MatchType matchType();
  }

  private static final class LikeMatchingEverything implements LikePattern {
    private static final LikeMatchingEverything INSTANCE = new LikeMatchingEverything();
    @Override public boolean matches(final String input) { return true; }
    @Override public MatchType matchType() { return MatchType.EVERYTHING; }
  }

  private static final class LikeWithNoMatch implements LikePattern {
    private static final LikeWithNoMatch INSTANCE = new LikeWithNoMatch();
    @Override public boolean matches(final String input) { return false; }
    @Override public MatchType matchType() { return MatchType.NOTHING; }
  }

  private static final class LikeWithStringEquals implements LikePattern {
    private final String expr;

    private LikeWithStringEquals(final String expr) {
      this.expr = expr;
    }

    @Override
    public MatchType matchType() {
      return MatchType.FULL;
    }

    @Override
    public boolean matches(final String input) {
      return StringUtil.equals(input, expr);
    }
  }

  private static final class RegexLikePattern implements LikePattern {
    private final Pattern pattern;
    private final MatchType matchType;

    private RegexLikePattern(final Pattern pattern, final boolean prefixMatch) {
      this.pattern = pattern;
      this.matchType = prefixMatch ? MatchType.PREFIX : MatchType.RANDOM;
    }

    @Override public MatchType matchType() {
      return matchType;
    }

    @Override
    public boolean matches(final String input) {
      return pattern.matcher(input).matches();
    }
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
  public static int compare(final String a, final String b) {
    if (a == b) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    return a.compareTo(b);
  }

  @Deprecated
  public static int compareTo(final String a, final String b) {
    return compare(a, b);
  }

  @SuppressWarnings("StringEquality")
  public static int compareIgnoreCase(final String a, final String b) {
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

  public static final Comparator<String> STRING_REVERSE_COMPARATOR = (a, b) -> StringUtil.compare(b, a);
}
