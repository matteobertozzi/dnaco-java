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

package tech.dnaco.bytes;

import java.util.Arrays;

import tech.dnaco.strings.StringUtil;

public final class BytesUtil {
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final byte[] NEW_LINE = new byte[] { '\n' };
  public static final byte[] CRLF = new byte[] { '\r', '\n' };

  private BytesUtil() {
    // no-op
  }

  // ================================================================================
  //  Bytes Length util
  // ================================================================================
  public static boolean isEmpty(final byte[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isNotEmpty(final byte[] input) {
    return (input != null) && (input.length != 0);
  }

  public static int length(final byte[] data) {
    return data != null ? data.length : 0;
  }

  // ================================================================================
  //  Bytes equals/compare util
  // ================================================================================
  public static boolean equals(final byte[] a, final byte[] b) {
    return (isEmpty(a) && isEmpty(b)) || Arrays.equals(a, b);
  }

  public static boolean equals(final byte[] a, final int aOff, final int aLen,
      final byte[] b, final int bOff, final int bLen) {
    return Arrays.equals(a, aOff, aOff + aLen, b, bOff, bOff + bLen);
  }

  public static int compare(final byte[] a, final byte[] b) {
    final int aLen = BytesUtil.length(a);
    final int bLen = BytesUtil.length(b);
    return compare(a, 0, aLen, b, 0, bLen);
  }

  public static int compare(final byte[] a, final int aOff, final int aLen,
      final byte[] b, final int bOff, final int bLen) {
    return Arrays.compare(a, aOff, aOff + aLen, b, bOff, bOff + bLen);
  }

  public static int prefix(final byte[] a, final int aOff, final int aLen,
      final byte[] b, final int bOff, final int bLen) {
    final int len = Math.min(aLen, bLen);
    for (int i = 0; i < len; ++i) {
      if (a[aOff + i] != b[bOff + i]) {
        return i;
      }
    }
    return len;
  }

  // ================================================================================
  //  Bytes concatenation
  // ================================================================================
  public static byte[] concat(final byte[]... data) {
    final byte[] fullData = new byte[length(data)];
    int offset = 0;
    for (int i = 0; i < data.length; ++i) {
      System.arraycopy(data[i], 0, fullData, offset, data[i].length);
      offset += data[i].length;
    }
    return fullData;
  }

  public static int length(final byte[]... data) {
    int length = 0;
    for (int i = 0; i < data.length; ++i) {
      length += data[i].length;
    }
    return length;
  }

  // ================================================================================
  //  Bytes to binary
  // ================================================================================
  public static String toBinaryString(final byte[] buf) {
    return toBinaryString(buf, 0, length(buf));
  }

  public static String toBinaryString(final byte[] buf, final int off, final int len) {
    final StringBuilder builder = new StringBuilder(len * 8);
    for (int i = 0; i < len; ++i) {
      if (i > 0) builder.append(' ');
      final int b = buf[off + i] & 0xff;
      for (int k = 7; k >= 0; --k) {
        builder.append((b & (1 << k)) != 0 ? '1' : '0');
      }
    }
    return builder.toString();
  }

  // ====================================================================================================
  //  chars[] helpers
  // ====================================================================================================
  public static char[] toChars(final byte[] bdata) {
    final char[] cdata = new char[bdata.length];
    for (int i = 0; i < bdata.length; ++i) {
      cdata[i] = (char)bdata[i];
    }
    return cdata;
  }

  public static byte[] fromChars(final char[] cdata) {
    final byte[] bdata = new byte[cdata.length];
    for (int i = 0; i < bdata.length; ++i) {
      bdata[i] = (byte)(cdata[i] & 0xff);
    }
    return bdata;
  }

  // ================================================================================
  //  Bytes to hex
  // ================================================================================
  public static byte[] fromHexString(final String data) {
    if(StringUtil.isEmpty(data)) return null;

    final int length = data.length();
    final byte[] buffer = new byte[length / 2];
    for (int i = 0; i < length; i += 2) {
      buffer[i / 2] = (byte) ((Character.digit(data.charAt(i), 16) << 4) + Character.digit(data.charAt(i+1), 16));
    }
    return buffer;
  }

  public static byte[] toHexBytes(final byte[] buf) {
    return toHexBytes(buf, 0, length(buf));
  }

  public static byte[] toHexBytes(final byte[] buf, final int off, final int len) {
    final byte[] hex = new byte[len * 2];
    for (int i = 0, j = 0; i < len; ++i, j += 2) {
      final int val = buf[off + i] & 0xff;
      hex[j] = (byte) StringUtil.HEX_DIGITS[(val >> 4) & 0xf];
      hex[j + 1] = (byte) StringUtil.HEX_DIGITS[val & 0xf];
    }
    return hex;
  }

  public static String toHexString(final byte[] buf) {
    return toHexString(buf, 0, buf.length);
  }

  public static String toHexString(final byte[] buf, final int off, final int len) {
    return toHexString(new StringBuilder(len * 2), buf, off, len).toString();
  }

  public static StringBuilder toHexString(final StringBuilder hex, final byte[] buf, final int off, final int len) {
    for (int i = 0; i < len; ++i) {
      final int val = buf[off + i] & 0xff;
      hex.append(StringUtil.HEX_DIGITS[(val >> 4) & 0xf]);
      hex.append(StringUtil.HEX_DIGITS[val & 0xf]);
    }
    return hex;
  }

  public static String toString(final byte[] buf) {
    return toString(buf, 0, buf.length);
  }

  public static String toString(final byte[] buf, final int off, final int len) {
    final StringBuilder builder = new StringBuilder(len * 2);
    for (int i = 0; i < len; ++i) {
      if (i > 0) builder.append(", ");
      builder.append(String.valueOf(buf[off + i]));
    }
    return builder.toString();
  }
}
