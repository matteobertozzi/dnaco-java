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

package tech.dnaco.util;

import java.security.SecureRandom;

import tech.dnaco.strings.StringUtil;

public final class RandData {
  private static class Holder {
    private static final SecureRandom secureRand = new SecureRandom();
  }

  private RandData() {
    // no-op
  }

  // ================================================================================
  //  Random Bytes
  // ================================================================================
  public static byte[] generateBytes(final int length) {
    return generateBytes(new byte[length]);
  }

  public static byte[] generateBytes(final byte[] buf) {
    Holder.secureRand.nextBytes(buf);
    return buf;
  }

  @SuppressWarnings("fallthrough")
  public static byte[] generateBytes(final byte[] buf, final int off, final int len) {
    if (off == 0 && buf.length == len) {
      Holder.secureRand.nextBytes(buf);
      return buf;
    }

    final SecureRandom rand = Holder.secureRand;
    int length = off + len;
    while (length >= 8) {
      final long v = rand.nextLong();
      buf[--length] = (byte)((v) & 0xff);
      buf[--length] = (byte)((v >>  8) & 0xff);
      buf[--length] = (byte)((v >> 16) & 0xff);
      buf[--length] = (byte)((v >> 24) & 0xff);
      buf[--length] = (byte)((v >> 32) & 0xff);
      buf[--length] = (byte)((v >> 40) & 0xff);
      buf[--length] = (byte)((v >> 48) & 0xff);
      buf[--length] = (byte)((v >> 56) & 0xff);
    }

    if (length > 0) {
      final long v = rand.nextLong();
      switch (length) {
        case 7: buf[--length] = (byte)((v) & 0xff);
        case 6: buf[--length] = (byte)((v >>  8) & 0xff);
        case 5: buf[--length] = (byte)((v >> 16) & 0xff);
        case 4: buf[--length] = (byte)((v >> 24) & 0xff);
        case 3: buf[--length] = (byte)((v >> 32) & 0xff);
        case 2: buf[--length] = (byte)((v >> 40) & 0xff);
        case 1: buf[--length] = (byte)((v >> 48) & 0xff);
      }
    }
    return buf;
  }

  // ================================================================================
  //  Random Chars
  // ================================================================================
  public static char[] generateAlphaNumericChars(final int length) {
    return generateChars(length, StringUtil.ALPHA_NUMERIC_CHARS);
  }

  public static String generateAlphaNumericString(final int length) {
    return new String(generateAlphaNumericChars(length));
  }

  public static char[] generateChars(final int length, final char[] charset) {
    return generateChars(new char[length], charset);
  }

  public static String generateString(final int length, final char[] charset) {
    return new String(generateChars(length, charset));
  }

  @SuppressWarnings("fallthrough")
  public static char[] generateChars(final char[] buffer, final char[] charset) {
    final SecureRandom rand = Holder.secureRand;
    int length = buffer.length;
    while (length >= 4) {
      final long v = rand.nextInt();
      buffer[--length] = charset[(int)((v) & 0xff) % charset.length];
      buffer[--length] = charset[(int)((v >>  8) & 0xff) % charset.length];
      buffer[--length] = charset[(int)((v >> 16) & 0xff) % charset.length];
      buffer[--length] = charset[(int)((v >> 20) & 0xff) % charset.length];
    }
    if (length > 0) {
      final long v = rand.nextInt();
      switch (length) {
        case 3: buffer[--length] = charset[(int)((v) & 0xff) % charset.length];
        case 2: buffer[--length] = charset[(int)((v >>  8) & 0xff) % charset.length];
        case 1: buffer[--length] = charset[(int)((v >> 16) & 0xff) % charset.length];
      }
    }
    return buffer;
  }
}
