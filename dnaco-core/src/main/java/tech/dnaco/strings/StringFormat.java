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
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StringFormat {
  private static final Pattern KEYWORD_PATTERN = Pattern.compile("\\{([-_a-zA-Z0-9]*)}");
  private static final Pattern POSITIONAL_PATTERN = Pattern.compile("\\{([0-9]+)}");

  private StringFormat() {
    // no-op
  }

  public static void main(final String[] args) throws Exception {
    System.out.println(StringFormat.format("{a {} b {xx} c}", 1, 2, 3));
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
    if (argsLen == 0) {
      msgBuilder.append(format);
      return 0;
    }

    int argsIndex = argsOff;
    final Matcher m = KEYWORD_PATTERN.matcher(format);
    while (m.find()) {
      final String key = m.group(1);
      final String value = stringValue(argsIndex < args.length ? args[argsIndex++] : "{unprovided arg}");
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
    final Matcher m = POSITIONAL_PATTERN.matcher(format);
    final StringBuilder buf = new StringBuilder(format.length() + (args.length * 8));
    while (m.find()) {
      final int pos = Integer.parseInt(m.group(1));
      final String value = stringValue(args[pos]);
      m.appendReplacement(buf, Matcher.quoteReplacement(value));
    }
    m.appendTail(buf);
    return buf.toString();
  }

  public static String stringValue(final Object value) {
    if (value instanceof byte[]) return Arrays.toString((byte[]) value);
    if (value instanceof int[]) return Arrays.toString((int[]) value);
    if (value instanceof long[]) return Arrays.toString((long[]) value);
    if (value instanceof float[]) return Arrays.toString((float[]) value);
    if (value instanceof double[]) return Arrays.toString((double[]) value);
    if (value instanceof Object[]) return Arrays.toString((Object[]) value);
    return String.valueOf(value);
  }
}
