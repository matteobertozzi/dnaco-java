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

package tech.dnaco.strings;

import java.math.BigInteger;
import java.util.Arrays;

import tech.dnaco.bytes.BytesUtil;

public final class BaseN {
  private static final BaseNTable BASE_35 = new BaseNTable("0123456789ABCDEFGHIJKLMNOPQRTUVWXYZ");
  private static final BaseNTable BASE_36 = new BaseNTable("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ");
  private static final BaseNTable BASE_52 = new BaseNTable("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
  private static final BaseNTable BASE_58 = new BaseNTable("123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ");
  private static final BaseNTable BASE_62 = new BaseNTable("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
  private static final BaseNTable BASE_90 = new BaseNTable("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&()*+,-./:;<=>?@[]^_{|}~");

  private BaseN() {
    // no-op
  }

  public static void main(final String[] args) throws Exception {
    final byte[] h = new byte[16];
    Arrays.fill(h, (byte)0xff);
    System.out.println(h.length + "/" + (h.length * 8));
    System.out.println(" - 32: " + BaseN.encodeBase32(h).length());
    System.out.println(" - 58: " + BaseN.encodeBase58(new BigInteger(1, h)).length());
    System.out.println(" - 62: " + BaseN.encodeBase62(new BigInteger(1, h)).length());
  }

  // --------------------------------------------------------------------------------
  //  Base32 related
  // --------------------------------------------------------------------------------
  public static String encodeBase32(final byte[] buf) {
    return encodeBase32(buf, 0, buf.length);
  }

  public static String encodeBase32(final byte[] buf, final int off, final int len) {
    return Base32.encode(Base32.STANDARD_ENCODING, buf, off, len);
  }

  public static String encodeHumanBase32(final byte[] buf) {
    return encodeHumanBase32(buf, 0, buf.length);
  }

  public static String encodeHumanBase32(final byte[] buf, final int off, final int len) {
    return Base32.encode(Base32.HUMAN_ENCODING, buf, off, len);
  }

  public static byte[] decodeHumanBase32(final String data) {
    return Base32.decode(data);
  }

  // ================================================================================
  //  Base36 encoder/decoder
  // ================================================================================
  public static String encodeBase36(final long value) {
    return encodeBaseN(BASE_36, value);
  }

  public static String encodeBase36(final byte[] value) {
    return encodeBase58(new BigInteger(value));
  }

  public static String encodeBase36(final BigInteger value) {
    return encodeBaseN(BASE_36, value);
  }

  public static long decodeBase36(final String value) {
    return decodeSmallBaseN(BASE_36, value);
  }

  public static BigInteger decodeBigBase36(final String value) {
    return decodeBaseN(BASE_36, value);
  }

  // ================================================================================
  //  Base52 encoder/decoder
  // ================================================================================
  public static String encodeBase52(final long value) {
    return encodeBaseN(BASE_52, value);
  }

  public static String encodeBase52(final byte[] value) {
    return encodeBase52(new BigInteger(value));
  }

  public static String encodeBase52(final BigInteger value) {
    return encodeBaseN(BASE_52, value);
  }

  public static long decodeBase52(final String value) {
    return decodeSmallBaseN(BASE_52, value);
  }

  public static BigInteger decodeBigBase52(final String value) {
    return decodeBaseN(BASE_52, value);
  }

  // ================================================================================
  //  Base58 encoder/decoder
  // ================================================================================
  public static String encodeBase58(final long value) {
    return encodeBaseN(BASE_58, value);
  }

  public static void encodeBase58(final StringBuilder result, final long value) {
    encodeBaseN(result, value, BASE_58.alphabet);
  }

  public static String encodeBase58(final byte[] value) {
    return encodeBase58(new BigInteger(value));
  }

  public static String encodeBase58(final BigInteger value) {
    return encodeBaseN(BASE_58, value);
  }

  public static long decodeBase58(final String value) {
    return decodeBase58(value, 0, value.length());
  }

  public static long decodeBase58(final String data, final int offset, final int length) {
    return decodeSmallBaseN(BASE_58, data, offset, length);
  }

  public static BigInteger decodeBigBase58(final String value) {
    return decodeBaseN(BASE_58, value);
  }

  // ================================================================================
  //  Base62 encoder/decoder
  // ================================================================================
  public static String encodeBase62(final long value) {
    return encodeBaseN(BASE_62, value);
  }

  public static String encodeBase62(final byte[] value) {
    return encodeBase62(new BigInteger(1, value));
  }

  public static String encodeBase62(final BigInteger value) {
    return encodeBaseN(BASE_62, value);
  }

  public static long decodeBase62(final String value) {
    return decodeSmallBaseN(BASE_62, value);
  }

  public static BigInteger decodeBigBase62(final String value) {
    return decodeBaseN(BASE_62, value);
  }

  // ================================================================================
  //  BaseN encoder/decoder
  // ================================================================================
  public static String encodeBaseN(final BaseNTable table, final long value) {
    return encodeBaseN(value, table.alphabet);
  }

  public static String encodeBaseN(final long value, final char[] dictionary) {
    final StringBuilder result = new StringBuilder();
    encodeBaseN(result, value, dictionary);
    return result.reverse().toString();
  }

  public static void encodeBaseN(final StringBuilder result, final long value, final char[] dictionary) {
    final int base = dictionary.length;
    long remaining = value;
    do {
      final int d = (int) Long.remainderUnsigned(remaining, base);
      remaining = Long.divideUnsigned(remaining, base);

      result.append(dictionary[d]);
    } while (remaining != 0);
  }

  public static String encodeBaseN(final BaseNTable table, final BigInteger value) {
    return encodeBaseN(value, table.alphabet);
  }

  public static String encodeBaseN(final BigInteger value, final char[] dictionary) {
    final BigInteger base = BigInteger.valueOf(dictionary.length);

    BigInteger remaining = value;
    if (remaining.compareTo(BigInteger.ZERO) < 0) {
      remaining = remaining.negate();
    }

    final StringBuilder result = new StringBuilder();
    do {
      final int d = remaining.remainder(base).intValue();
      remaining = remaining.divide(base);

      result.append(dictionary[d]);
    } while (!remaining.equals(BigInteger.ZERO));
    return result.reverse().toString();
  }

  public static long decodeSmallBaseN(final BaseNTable table, final String data) {
    return decodeSmallBaseN(table, data, 0, data.length());
  }

  public static long decodeSmallBaseN(final BaseNTable table, final String data, final int offset, final int length) {
    final int[] decoder = table.decodeMap;
    final long[] pows = table.pows;

    if (length > pows.length) {
      throw new IllegalArgumentException("too large to fit. length:" + length + " max:" + pows.length);
    }

    long v = 0;
    for (int i = 0; i < length; ++i) {
      final char c = data.charAt(offset + (length - 1) - i);
      final long x = decoder[c];
      if (x < 0) throw new IllegalArgumentException("invalid char " + c + " at position " + i);
      v += pows[i] * x;
    }
    return v;
  }

  public static BigInteger decodeBaseN(final String str, final char[] dictionary) {
    final BigInteger[] dictMap = new BigInteger[128];
    for (int i = 0; i < dictionary.length; ++i) {
      dictMap[dictionary[i]] = BigInteger.valueOf(i);
    }
    return decodeBaseN(str, dictionary, dictMap);
  }

  public static BigInteger decodeBaseN(final BaseNTable table, final String str) {
    return decodeBaseN(str, table.alphabet, table.bigDecodeMap);
  }

  public static BigInteger decodeBaseN(final String str, final char[] dictionary, final BigInteger[] dictMap) {
    final int lastIndex = str.length() - 1;
    BigInteger bi = BigInteger.ZERO;
    final BigInteger base = BigInteger.valueOf(dictionary.length);
    for (int i = 0, exp = 0; i <= lastIndex; ++exp, ++i) {
      final BigInteger a = dictMap[str.charAt(lastIndex - i)];
      final BigInteger b = base.pow(exp).multiply(a);
      bi = bi.add(b);
    }
    return bi;
  }

  // ================================================================================
  //  BaseN precomputed
  // ================================================================================
  private static final class BaseNTable {
    private final BigInteger[] bigDecodeMap;
    private final int[] decodeMap;
    private final long[] pows;
    private final char[] alphabet;

    private BaseNTable(final String alphabet) {
      this(alphabet.toCharArray());
    }

    private BaseNTable(final char[] alphabet) {
      this.alphabet = alphabet;

      this.pows = computePows(alphabet);
      this.bigDecodeMap = new BigInteger[128];
      this.decodeMap = new int[128];
      Arrays.fill(decodeMap, -1);
      for (int i = 0; i < alphabet.length; ++i) {
        this.bigDecodeMap[alphabet[i]] = BigInteger.valueOf(i);
        this.decodeMap[alphabet[i]] = i;
      }
    }

    private static long[] computePows(final char[] alphabet) {
      final long base = alphabet.length;
      long remaining = Long.MAX_VALUE;
      int count = 0;
      do {
        remaining = Long.divideUnsigned(remaining, base);
        count++;
      } while (remaining != 0);

      final long[] pows = new long[count];
      for (int i = 0, exp = 0; i < pows.length; ++exp, ++i) {
        pows[i] = pow(base, exp);
      }
      return pows;
    }

    private static long pow(final long base, long exp) {
      if (exp == 0) return 1;
      long result = base;
      while (exp --> 1) {
        result *= base;
      }
      return result;
    }
  }

  private static final class Base32 {
    private static final char[] STANDARD_ENCODING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".toCharArray();
    private static final char[] HUMAN_ENCODING = "ybndrfg8ejkmcpqxot1uwisza345h769".toCharArray();

    private static final int[] HUMAN_DECODING = new int[] {
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  18,  -1,  25,  26,  27,  30,  29,   7,  31,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  24,   1,  12,   3,   8,   5,   6,  28,  21,   9,  10,  -1,  11,
       2,  16,  13,  14,   4,  22,  17,  19,  -1,  20,  15,   0,  23,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,
      -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1
    };

    private Base32() {
      // no-op
    }

    private static String encode(final char[] dictionary, final byte[] data, final int offset, int length) {
      if (BytesUtil.isEmpty(data)) return null;

      final StringBuilder builder = new StringBuilder(getEncodedLength(length));

      int i = offset;
      while (length >= 5) {
        final long x = ((long)(data[i] & 0xff) << 32)
                     | ((long)(data[i + 1] & 0xff) << 24)
                     | (data[i + 2] & 0xff) << 16
                     | (data[i + 3] & 0xff) << 8
                     | (data[i + 4] & 0xff);
        builder.append(dictionary[(int)((x >> 35) & 0x1f)]);
        builder.append(dictionary[(int)((x >> 30) & 0x1f)]);
        builder.append(dictionary[(int)((x >> 25) & 0x1f)]);
        builder.append(dictionary[(int)((x >> 20) & 0x1f)]);
        builder.append(dictionary[(int)((x >> 15) & 0x1f)]);
        builder.append(dictionary[(int)((x >> 10) & 0x1f)]);
        builder.append(dictionary[(int)((x >>  5) & 0x1f)]);
        builder.append(dictionary[(int)((x) & 0x1f)]);
        length -= 5;
        i += 5;
      }

      switch (length) {
        case 4: // 7byte
          final long x4 = ((long)(data[i] & 0xff) << 27)
                        | (data[i + 1] & 0xff) << 19
                        | (data[i + 2] & 0xff) << 11
                        | (data[i + 3] & 0xff) << 3;
          builder.append(dictionary[(int)((x4 >> 30) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >> 25) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >> 20) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >> 15) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >> 10) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >>  5) & 0x1f)]);
          builder.append(dictionary[(int)((x4) & 0x1f)]);
          break;
        case 3: // 5byte
          final int x3 =  (data[i] & 0xff) << 17 |
                          (data[i + 1] & 0xff) << 9  |
                          (data[i + 2] & 0xff) << 1;
          builder.append(dictionary[(x3 >> 20) & 0x1f]);
          builder.append(dictionary[(x3 >> 15) & 0x1f]);
          builder.append(dictionary[(x3 >> 10) & 0x1f]);
          builder.append(dictionary[(x3 >>  5) & 0x1f]);
          builder.append(dictionary[(x3) & 0x1f]);
          break;
        case 2: // 4byte
          final int x2 = ((data[i] & 0xff) << 12) | ((data[i + 1] & 0xff) << 4);
          builder.append(dictionary[(x2 >> 15) & 0x1f]);
          builder.append(dictionary[(x2 >> 10) & 0x1f]);
          builder.append(dictionary[(x2 >>  5) & 0x1f]);
          builder.append(dictionary[(x2) & 0x1f]);
          break;
        case 1: // 2byte
          final int x1 = (data[i] & 0xff) << 2;
          builder.append(dictionary[(x1 >>  5) & 0x1f]);
          builder.append(dictionary[(x1) & 0x1f]);
          break;
      }

      return builder.toString();
    }

    public static byte[] decode(final String encoded) {
      return StringUtil.isEmpty(encoded) ? null : decode(encoded.getBytes(), 0, encoded.length());
    }

    public static byte[] decode(final byte[] encoded, final int offset, int length) {
      if (BytesUtil.isEmpty(encoded)) return encoded;

      final byte[] data = new byte[getDecodedLength(length)];
      int dataIndex = 0;

      int i = offset;
      while (length >= 8) {
        final long x5 = ((long)(HUMAN_DECODING[encoded[i] & 0xff]) << 35)
                      | ((long)(HUMAN_DECODING[encoded[i + 1] & 0xff]) << 30)
                      | ((long)HUMAN_DECODING[encoded[i + 2] & 0xff]) << 25
                      | (HUMAN_DECODING[encoded[i + 3] & 0xff] << 20)
                      | (HUMAN_DECODING[encoded[i + 4] & 0xff] << 15)
                      | (HUMAN_DECODING[encoded[i + 5] & 0xff] << 10)
                      | (HUMAN_DECODING[encoded[i + 6] & 0xff] <<  5)
                      | (HUMAN_DECODING[encoded[i + 7] & 0xff]);
            data[dataIndex++] = (byte)((x5 >> 32) & 0xff);
            data[dataIndex++] = (byte)((x5 >> 24) & 0xff);
            data[dataIndex++] = (byte)((x5 >> 16) & 0xff);
            data[dataIndex++] = (byte)((x5 >> 8) & 0xff);
            data[dataIndex++] = (byte)((x5) & 0xff);

        i += 8;
        length -= 8;
      }

      switch (length) {
        case 7:
          final long x4 = ((long)(HUMAN_DECODING[encoded[i] & 0xff]) << 30)
                        | ((long)(HUMAN_DECODING[encoded[i + 1] & 0xff]) << 25)
                        | (HUMAN_DECODING[encoded[i + 2] & 0xff] << 20)
                        | (HUMAN_DECODING[encoded[i + 3] & 0xff] << 15)
                        | (HUMAN_DECODING[encoded[i + 4] & 0xff] << 10)
                        | (HUMAN_DECODING[encoded[i + 5] & 0xff] <<  5)
                        | (HUMAN_DECODING[encoded[i + 6] & 0xff]);
          data[dataIndex++] = (byte)((x4 >> 27) & 0xff);
          data[dataIndex++] = (byte)((x4 >> 19) & 0xff);
          data[dataIndex++] = (byte)((x4 >> 11) & 0xff);
          data[dataIndex++] = (byte)((x4 >> 3) & 0xff);
          break;
        case 5:
          final int x3 = (HUMAN_DECODING[encoded[i] & 0xff] << 20)
                       | (HUMAN_DECODING[encoded[i + 1] & 0xff] << 15)
                       | (HUMAN_DECODING[encoded[i + 2] & 0xff] << 10)
                       | (HUMAN_DECODING[encoded[i + 3] & 0xff] <<  5)
                       | (HUMAN_DECODING[encoded[i + 4] & 0xff]);
          data[dataIndex++] = (byte)((x3 >> 17) & 0xff);
          data[dataIndex++] = (byte)((x3 >> 9) & 0xff);
          data[dataIndex++] = (byte)((x3 >> 1) & 0xff);
          break;
        case 4:
          final int x2 = (HUMAN_DECODING[encoded[i] & 0xff] << 15)
                       | (HUMAN_DECODING[encoded[i + 1] & 0xff] << 10)
                       | (HUMAN_DECODING[encoded[i + 2] & 0xff] <<  5)
                       | (HUMAN_DECODING[encoded[i + 3] & 0xff]);
          data[dataIndex++] = (byte)((x2 >> 12) & 0xff);
          data[dataIndex++] = (byte)((x2 >> 4) & 0xff);
          break;
        case 2:
          final int x1 = (HUMAN_DECODING[encoded[i] & 0xff] << 5)
                       | (HUMAN_DECODING[encoded[i + 1] & 0xff]);
          data[dataIndex++] = (byte)((x1 >> 2) & 0xff);
          break;
      }

      return data;
    }

    public static boolean isBase32Encoded(final int expectedRawLen, final String data, final int offset, final int length) {
      final int encodeLength = getDecodedLength(length);
      if (length != encodeLength) return false;

      for (int i = 0; i < length; ++i) {
        final char b = data.charAt(offset + i);
        if (b >= HUMAN_DECODING.length || HUMAN_DECODING[b] < 0) {
          return false;
        }
      }
      return true;
    }

    private static int getDecodedLength(final int encodedLength) {
      final int length = (encodedLength / 8) * 5;
      switch (encodedLength % 8) {
        case 7: return length + 4;
        case 5: return length + 3;
        case 4: return length + 2;
        case 2: return length + 1;
      }
      return length;
    }

    private static int getEncodedLength(final int decodedLength) {
      final int length = (decodedLength / 5) * 8;
      switch (decodedLength % 5) {
        case 4: return length + 7;
        case 3: return length + 5;
        case 2: return length + 4;
        case 1: return length + 2;
      }
      return length;
    }
  }
}
