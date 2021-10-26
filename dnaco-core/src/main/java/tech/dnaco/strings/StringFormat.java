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

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StringFormat {
  private static final Pattern KEYWORD_PATTERN = Pattern.compile("\\{([-_a-zA-Z0-9]*)}");
  private static final Pattern POSITIONAL_PATTERN = Pattern.compile("\\{(([0-9]+)(\\:[-_a-zA-Z0-9]+)*)}");

  private StringFormat() {
    // no-op
  }

  public static String format(final String format, final Object... args) {
    final StringBuilder builder = new StringBuilder(format.length() + (args.length * 8));
    applyFormat(builder, format, args);
    return builder.toString();
  }

  public static String format(final String format, final Supplier<?>... args) {
    final Object[] params = new Object[args.length];
    for (int i = 0, n = args.length; i < n; ++i) {
      params[i] = args[i].get();
    }

    final StringBuilder builder = new StringBuilder(format.length() + (params.length * 8));
    applyFormat(builder, format, params);
    return builder.toString();
  }

  public static int applyFormat(final StringBuilder msgBuilder, final String format, final Object[] args) {
    return applyFormat(msgBuilder, format, args, 0, args != null ? args.length : 0);
  }

  public static int applyFormat(final StringBuilder msgBuilder, final String format,
      final Object[] args, final int argsOff, final int argsLen) {
    return applyFormat(msgBuilder, format, args, argsOff, argsLen, StringFormat::valueOf);
  }

  public static int applyFormat(final StringBuilder msgBuilder, final String format,
      final Object[] args, final int argsOff, final int argsLen,
      final Function<Object, String> valueToString) {
    if (argsLen == 0) {
      msgBuilder.append(format);
      return 1;
    }

    int argsIndex = argsOff;
    final Matcher m = KEYWORD_PATTERN.matcher(format);
    while (m.find()) {
      final String key = m.group(1);
      final String value = argsIndex < args.length ? valueToString.apply(args[argsIndex++]) : "{unprovided arg}";
      if (StringUtil.isNotEmpty(key)) {
        m.appendReplacement(msgBuilder, key);
        msgBuilder.append(':').append(value);
      } else {
        m.appendReplacement(msgBuilder, Matcher.quoteReplacement(value));
      }
    }
    m.appendTail(msgBuilder);
    return argsIndex;
  }

  public static int indexOfKeywordArgument(final String format, final String key) {
    final Matcher m = KEYWORD_PATTERN.matcher(format);
    for (int i = 0; m.find(); ++i) {
      if (StringUtil.equals(key, m.group(1))) {
        return i;
      }
    }
    return -1;
  }

  public static String positionalFormat(final String format, final Object... args) {
    final StringBuilder buf = new StringBuilder(format.length() + (args.length * 8));
    applyPositionalFormat(buf, format, args, 0, args.length, StringFormat::valueOf);
    return buf.toString();
  }

  public static void applyPositionalFormat(final StringBuilder msgBuilder, final String format, final Object[] args) {
    applyPositionalFormat(msgBuilder, format, args, 0, args != null ? args.length : 0, StringFormat::valueOf);
  }

  public static void applyPositionalFormat(final StringBuilder msgBuilder, final String format,
      final Object[] args, final int argsOff, final int argsLen,
      final Function<Object, String> valueToString) {
    if (argsLen == 0) {
      msgBuilder.append(format);
      return;
    }

    final Matcher m = POSITIONAL_PATTERN.matcher(format);
    while (m.find()) {
      final int argsIndex = Integer.parseInt(m.group(2));
      final String value = argsIndex < args.length ? valueToString.apply(args[argsIndex]) : "{unprovided arg}";
      m.appendReplacement(msgBuilder, Matcher.quoteReplacement(value));
    }
    m.appendTail(msgBuilder);
  }

  public static String valueOf(final Object value) {
    if (value == null) return "null";
    if (value.getClass().isArray()) {
      if (value instanceof byte[] bArray) return Arrays.toString(bArray);
      if (value instanceof int[] iArray) return Arrays.toString(iArray);
      if (value instanceof long[] lArray) return Arrays.toString(lArray);
      if (value instanceof float[] fArray) return Arrays.toString(fArray);
      if (value instanceof double[] dArrary) return Arrays.toString(dArrary);
      if (value instanceof Object[] oArray) return Arrays.toString(oArray);
    }
    if (value instanceof Map<?, ?> map) return stringMapValue(map);
    return String.valueOf(value);
  }

  private static String stringMapValue(final Map<?, ?> map) {
    final StringBuilder builder = new StringBuilder(map.size() * 8);
    builder.append("{");
    int index = 0;
    for (final Entry<?, ?> entry: map.entrySet()) {
      if (index++ > 0) builder.append(", ");
      builder.append(entry.getKey());
      builder.append(":");
      builder.append(valueOf(entry.getValue()));
    }
    builder.append("}");
    return builder.toString();
  }

  public static void main(String[] args) {
    long startTime = System.nanoTime();
    StringBuilder builder = new StringBuilder(1 << 20);
    for (int i = 0; i < 100_000_000; ++i) {
      builder.append(valueOf(new int[] { i }));
    }
    long endTime = System.nanoTime();
    System.out.println(builder.length() + " -> " + HumansUtil.humanTimeNanos(endTime - startTime));
  }
}
