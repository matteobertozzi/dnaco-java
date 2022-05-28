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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import tech.dnaco.collections.arrays.ArrayUtil;
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
  public static int length(final byte[] data) {
    return data != null ? data.length : 0;
  }

  public static boolean isEmpty(final byte[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isNotEmpty(final byte[] input) {
    return (input != null) && (input.length != 0);
  }

  // ================================================================================
  //  Bytes Check
  // ================================================================================
  public static boolean isFilledWithZeros(final byte[] data) {
    return isFilledWith(data, 0);
  }

  public static boolean isFilledWith(final byte[] data, final int value) {
    for (int i = 0; i < data.length; ++i) {
      if ((data[i] & 0xff) != value) {
        return false;
      }
    }
    return true;
  }


  // ================================================================================
  //  Bytes find single byte util
  // ================================================================================
  public static int indexOf(final byte[] haystack, final byte needle) {
    return indexOf(haystack, 0, haystack.length, needle);
  }

  public static int indexOf(final byte[] haystack, final int haystackOff, final byte needle) {
    return indexOf(haystack, haystackOff, haystack.length - haystackOff, needle);
  }

  public static int indexOf(final byte[] haystack, final int haystackOff, final int haystackLen, final byte needle) {
    for (int i = 0; i < haystackLen; ++i) {
      if (haystack[haystackOff + i] == needle) {
        return haystackOff + i;
      }
    }
    return -1;
  }

  public static int lastIndexOf(final byte[] haystack, final byte needle) {
    return lastIndexOf(haystack, 0, haystack.length, needle);
  }

  public static int lastIndexOf(final byte[] haystack, final int haystackOff, final byte needle) {
    return lastIndexOf(haystack, haystackOff, haystack.length - haystackOff, needle);
  }

  public static int lastIndexOf(final byte[] haystack, final int haystackOff, final int haystackLen, final byte needle) {
    for (int i = haystackLen - 1; i >= 0; --i) {
      if (haystack[haystackOff + i] == needle) {
        return haystackOff + i;
      }
    }
    return -1;
  }

  // ================================================================================
  //  Bytes find multi byte util
  // ================================================================================
  public static int indexOf(final byte[] haystack, final byte[] needle) {
    return indexOf(haystack, 0, haystack.length, needle, needle.length);
  }

  public static int indexOf(final byte[] haystack, final int haystackOff, final byte[] needle) {
    return indexOf(haystack, haystackOff, haystack.length - haystackOff, needle, needle.length);
  }

  public static int indexOf(final byte[] haystack, final int haystackOff, final int haystackLen, final byte[] needle) {
    return indexOf(haystack, haystackOff, haystackLen, needle, needle.length);
  }

  private static int indexOf(final byte[] haystack, final int haystackOff, final int haystackLen,
      final byte[] needle, final int needleLen) {
    if (needleLen > haystackLen || needleLen == 0 || haystackLen == 0) return -1;
    if (needleLen == 1) return indexOf(haystack, haystackOff, haystackLen, needle[0]);

    final int len = haystackLen - needleLen;
    for (int i = 0; i < len; ++i) {
      final int off = haystackOff + i;
      if (Arrays.equals(haystack, off, off + needleLen, needle, 0, needleLen)) {
        return off;
      }
    }
    return -1;
  }

  public static int lastIndexOf(final byte[] haystack, final byte[] needle) {
    return lastIndexOf(haystack, 0, haystack.length, needle, needle.length);
  }

  public static int lastIndexOf(final byte[] haystack, final int haystackOff, final byte[] needle) {
    return lastIndexOf(haystack, haystackOff, haystack.length - haystackOff, needle, needle.length);
  }

  public static int lastIndexOf(final byte[] haystack, final int haystackOff, final int haystackLen, final byte[] needle) {
    return lastIndexOf(haystack, haystackOff, haystackLen, needle, needle.length);
  }

  private static int lastIndexOf(final byte[] haystack, final int haystackOff, final int haystackLen,
      final byte[] needle, final int needleLen) {
    if (needleLen > haystackLen || needleLen == 0 || haystackLen == 0) return -1;
    if (needleLen == 1) return lastIndexOf(haystack, haystackOff, haystackLen, needle[0]);

    final int len = haystackLen - needleLen;
    for (int i = len; i >= 0; --i) {
      final int off = haystackOff + i;
      if (Arrays.equals(haystack, off, off + needleLen, needle, 0, needleLen)) {
        return off;
      }
    }
    return -1;
  }

  // ================================================================================
  //  Bytes to int
  // ================================================================================
  public static int parseUnsignedInt(final byte[] data, final int off, final int count)
      throws NumberFormatException {
    return Math.toIntExact(parseUnsignedLong(data, off, count));
  }

  public static long parseUnsignedLong(final byte[] data, final int off, final int count)
      throws NumberFormatException {
    long value = 0;
    for (int i = 0; i < count; ++i) {
      final int chr = data[off + i];
      if (chr >= 48 && chr <= 57) {
        value = value * 10 + (chr - 48);
      } else {
        throw new NumberFormatException();
      }
    }
    return value;
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
    return Arrays.compareUnsigned(a, aOff, aOff + aLen, b, bOff, bOff + bLen);
  }

  // ================================================================================
  //  Prefix Util
  // ================================================================================
  public static byte[] increaseOne(final byte[] bytes) throws Exception {
    final byte BYTE_MAX_VALUE = (byte) 0xff;
    assert bytes.length > 0;
    final byte last = bytes[bytes.length - 1];
    if (last != BYTE_MAX_VALUE) {
      bytes[bytes.length - 1] += 0x01;
    } else {
      // Process overflow (like [1, 255] => [2, 0])
      int i = bytes.length - 1;
      for (; i > 0 && bytes[i] == BYTE_MAX_VALUE; --i) {
        bytes[i] += 0x01;
      }
      if (bytes[i] == BYTE_MAX_VALUE) {
        assert i == 0;
        throw new Exception("Unable to increase bytes: " + BytesUtil.toHexString(bytes));
      }
      bytes[i] += 0x01;
    }
    return bytes;
  }

  public static byte[] prefixEndKey(final byte[] prefix) throws Exception {
    final byte[] endKey = Arrays.copyOf(prefix, prefix.length);
    return increaseOne(endKey);
  }

  public static int prefix(final byte[] a, final byte[] b) {
    return prefix(a, 0, a.length, b, 0, b.length);
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

  public static boolean hasPrefix(final byte[] buf, final byte[] prefix) {
    return hasPrefix(buf, 0, length(buf), prefix, 0, length(prefix));
  }

  public static boolean hasPrefix(final byte[] buf, final int off, final int len,
      final byte[] prefix, final int prefixOff, final int prefixLen) {
    return prefix(buf, off, len, prefix, prefixOff, prefixLen) == prefixLen;
  }

  public static int suffix(final byte[] a, final byte[] b) {
    return suffix(a, 0, a.length, b, 0, b.length);
  }

  public static int suffix(final byte[] a, final int aOff, final int aLen,
      final byte[] b, final int bOff, final int bLen) {
    final int len = Math.min(aLen, bLen);
    for (int i = 1; i <= len; ++i) {
      if ((a[aLen - i] & 0xff) != (b[bLen - i] & 0xff)) {
        return i - 1;
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
  //  String to Bytes
  // ================================================================================
  public static byte[][] toBytes(final String[] values) {
    return toBytes(values, StandardCharsets.UTF_8);
  }

  public static byte[][] toBytes(final String[] values, final Charset charsets) {
    if (ArrayUtil.isEmpty(values)) return new byte[0][];

    final byte[][] bValues = new byte[values.length][];
    for (int i = 0; i < values.length; ++i) {
      bValues[i] = values[i].getBytes(charsets);
    }
    return bValues;
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
    if (StringUtil.isEmpty(data)) return null;

    final int length = data.length();
    final byte[] buffer = new byte[length >> 1];
    for (int i = 0; i < length; i += 2) {
      buffer[i >> 1] = (byte) ((Character.digit(data.charAt(i), 16) << 4) + Character.digit(data.charAt(i+1), 16));
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
    if (len == 0) return "[]";

    final StringBuilder builder = new StringBuilder(len * 2);
    builder.append("[");
    for (int i = 0; i < len; ++i) {
      if (i > 0) builder.append(", ");
      builder.append(String.valueOf(buf[off + i]));
    }
    builder.append("]");
    return builder.toString();
  }
}
