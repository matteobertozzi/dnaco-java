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

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.HashMap;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.util.RandData;

public final class BaseN {
  public static final char[] BASE_58 = "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ".toCharArray();
  public static final char[] BASE_62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  private BaseN() {
    // no-op
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

  // --------------------------------------------------------------------------------
  //  Base36 related
  // --------------------------------------------------------------------------------
  public static String encodeBase36(final long value) {
    return Long.toString(value, 36);
  }

  public static String encodeBase36(final BigInteger value) {
    return value.toString(36);
  }

  public static String encodeBase36(final byte[] value) {
    return encodeBase36(new BigInteger(value));
  }

  public static long decodeBase36(final String value) {
    return Long.parseLong(value, 36);
  }

  public static BigInteger decodeBigBase36(final String value) {
    return new BigInteger(value, 36);
  }

  // --------------------------------------------------------------------------------
  //  Base58 related
  // --------------------------------------------------------------------------------
  public static String encodeBase58(final long value) {
    return encodeBaseN(value, BASE_58);
  }

  public static String encodeBase58(final BigInteger value) {
    return encodeBaseN(value, BASE_58);
  }

  public static String encodeBase58(final byte[] value) {
    return encodeBase58(new BigInteger(value));
  }

  public static long decodeBase58(final String value) {
    return decodeBaseN(value, BASE_58).longValue();
  }

  public static BigInteger decodeBigBase58(final String value) {
    return decodeBaseN(value, BASE_58);
  }

  // --------------------------------------------------------------------------------
  //  Base62 related
  // --------------------------------------------------------------------------------
  public static String encodeBase62(final long value) {
    return encodeBaseN(value, BASE_62);
  }

  public static String encodeBase62(final BigInteger value) {
    return encodeBaseN(value, BASE_62);
  }

  public static String encodeBase62(final byte[] value) {
    return encodeBase62(new BigInteger(value));
  }

  public static long decodeBase62(final String value) {
    return decodeBaseN(value, BASE_62).longValue();
  }

  public static BigInteger decodeBigBase62(final String value) {
    return decodeBaseN(value, BASE_62);
  }

  // ================================================================================
  //  BaseN encoder/decoder
  // ================================================================================
  public static String encodeBaseN(final long value, final char[] dictionary) {
    final int base = dictionary.length;
    long remaining = value < 0 ? -value : value;

    final StringBuilder result = new StringBuilder();
    while (true) {
      final int d = (int) (remaining % base);
      remaining /= base;

      result.append(dictionary[d]);
      if (remaining == 0) {
        break;
      }
    }
    return result.reverse().toString();
  }

  public static String encodeBaseN(final BigInteger value, final char[] dictionary) {
    final BigInteger base = BigInteger.valueOf(dictionary.length);

    BigInteger remaining = value;
    if (remaining.compareTo(BigInteger.ZERO) < 0) {
      remaining = remaining.negate();
    }

    final StringBuilder result = new StringBuilder();
    while (true) {
      final int d = remaining.remainder(base).intValue();
      remaining = remaining.divide(base);

      result.append(dictionary[d]);
      if (remaining.equals(BigInteger.ZERO)) {
        break;
      }
    }
    return result.reverse().toString();
  }

  public static BigInteger decodeBaseN(final String str, final char[] dictionary) {
    final char[] chars = new char[str.length()];
    for (int i = 0; i < chars.length; ++i) {
      chars[chars.length - 1 - i] = str.charAt(i);
    }

    // TODO: move out/cache for dict
    final HashMap<Character, BigInteger> dictMap = new HashMap<>(dictionary.length);
    for (int i = 0; i < dictionary.length; ++i) {
      dictMap.put(dictionary[i], BigInteger.valueOf(i));
    }

    BigInteger bi = BigInteger.ZERO;
    final BigInteger base = BigInteger.valueOf(dictionary.length);
    for (int i = 0, exp = 0; i < chars.length; ++exp, ++i) {
      final BigInteger a = dictMap.get(chars[i]);
      final BigInteger b = base.pow(exp).multiply(a);
      bi = bi.add(b);
    }
    return bi;
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
        final long x = ((long)(data[i + 0] & 0xff) << 32)
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
        builder.append(dictionary[(int)((x >>  0) & 0x1f)]);
        length -= 5;
        i += 5;
      }

      switch (length) {
        case 4: // 7byte
          final long x4 = ((long)(data[i + 0] & 0xff) << 27)
                        | (data[i + 1] & 0xff) << 19
                        | (data[i + 2] & 0xff) << 11
                        | (data[i + 3] & 0xff) << 3;
          builder.append(dictionary[(int)((x4 >> 30) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >> 25) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >> 20) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >> 15) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >> 10) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >>  5) & 0x1f)]);
          builder.append(dictionary[(int)((x4 >>  0) & 0x1f)]);
          break;
        case 3: // 5byte
          final int x3 =  (data[i + 0] & 0xff) << 17 |
                          (data[i + 1] & 0xff) << 9  |
                          (data[i + 2] & 0xff) << 1;
          builder.append(dictionary[(x3 >> 20) & 0x1f]);
          builder.append(dictionary[(x3 >> 15) & 0x1f]);
          builder.append(dictionary[(x3 >> 10) & 0x1f]);
          builder.append(dictionary[(x3 >>  5) & 0x1f]);
          builder.append(dictionary[(x3 >>  0) & 0x1f]);
          break;
        case 2: // 4byte
          final int x2 = ((data[i + 0] & 0xff) << 12) | ((data[i + 1] & 0xff) << 4);
          builder.append(dictionary[(x2 >> 15) & 0x1f]);
          builder.append(dictionary[(x2 >> 10) & 0x1f]);
          builder.append(dictionary[(x2 >>  5) & 0x1f]);
          builder.append(dictionary[(x2 >>  0) & 0x1f]);
          break;
        case 1: // 2byte
          final int x1 = (data[i + 0] & 0xff) << 2;
          builder.append(dictionary[(x1 >>  5) & 0x1f]);
          builder.append(dictionary[(x1 >>  0) & 0x1f]);
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
        final long x5 = ((long)(HUMAN_DECODING[encoded[i + 0] & 0xff]) << 35)
                      | ((long)(HUMAN_DECODING[encoded[i + 1] & 0xff]) << 30)
                      | ((long)HUMAN_DECODING[encoded[i + 2] & 0xff]) << 25
                      | (HUMAN_DECODING[encoded[i + 3] & 0xff] << 20)
                      | (HUMAN_DECODING[encoded[i + 4] & 0xff] << 15)
                      | (HUMAN_DECODING[encoded[i + 5] & 0xff] << 10)
                      | (HUMAN_DECODING[encoded[i + 6] & 0xff] <<  5)
                      | (HUMAN_DECODING[encoded[i + 7] & 0xff] <<  0);
            data[dataIndex++] = (byte)((x5 >> 32) & 0xff);
            data[dataIndex++] = (byte)((x5 >> 24) & 0xff);
            data[dataIndex++] = (byte)((x5 >> 16) & 0xff);
            data[dataIndex++] = (byte)((x5 >> 8) & 0xff);
            data[dataIndex++] = (byte)((x5 >> 0) & 0xff);

        i += 8;
        length -= 8;
      }

      switch (length) {
        case 7:
          final long x4 = ((long)(HUMAN_DECODING[encoded[i + 0] & 0xff]) << 30)
                        | ((long)(HUMAN_DECODING[encoded[i + 1] & 0xff]) << 25)
                        | (HUMAN_DECODING[encoded[i + 2] & 0xff] << 20)
                        | (HUMAN_DECODING[encoded[i + 3] & 0xff] << 15)
                        | (HUMAN_DECODING[encoded[i + 4] & 0xff] << 10)
                        | (HUMAN_DECODING[encoded[i + 5] & 0xff] <<  5)
                        | (HUMAN_DECODING[encoded[i + 6] & 0xff] <<  0);
          data[dataIndex++] = (byte)((x4 >> 27) & 0xff);
          data[dataIndex++] = (byte)((x4 >> 19) & 0xff);
          data[dataIndex++] = (byte)((x4 >> 11) & 0xff);
          data[dataIndex++] = (byte)((x4 >> 3) & 0xff);
          break;
        case 5:
          final int x3 = (HUMAN_DECODING[encoded[i + 0] & 0xff] << 20)
                       | (HUMAN_DECODING[encoded[i + 1] & 0xff] << 15)
                       | (HUMAN_DECODING[encoded[i + 2] & 0xff] << 10)
                       | (HUMAN_DECODING[encoded[i + 3] & 0xff] <<  5)
                       | (HUMAN_DECODING[encoded[i + 4] & 0xff] <<  0);
          data[dataIndex++] = (byte)((x3 >> 17) & 0xff);
          data[dataIndex++] = (byte)((x3 >> 9) & 0xff);
          data[dataIndex++] = (byte)((x3 >> 1) & 0xff);
          break;
        case 4:
          final int x2 = (HUMAN_DECODING[encoded[i + 0] & 0xff] << 15)
                       | (HUMAN_DECODING[encoded[i + 1] & 0xff] << 10)
                       | (HUMAN_DECODING[encoded[i + 2] & 0xff] <<  5)
                       | (HUMAN_DECODING[encoded[i + 3] & 0xff] <<  0);
          data[dataIndex++] = (byte)((x2 >> 12) & 0xff);
          data[dataIndex++] = (byte)((x2 >> 4) & 0xff);
          break;
        case 2:
          final int x1 = (HUMAN_DECODING[encoded[i + 0] & 0xff] << 5)
                       | (HUMAN_DECODING[encoded[i + 1] & 0xff] << 0);
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

  public static void main(String[] args) throws Exception {
    for (int i = 0; i < 10; ++i) {
      final MessageDigest h = MessageDigest.getInstance("SHA3-256");
      h.update(RandData.generateBytes(512));
      final byte[] buf = h.digest();
      final BigInteger n = new BigInteger(buf);
      //final BigInteger n = new BigInteger(buf);

      long startTime = System.nanoTime();
      final String n1 = encodeBaseN(n, BASE_62);
      long endTime = System.nanoTime() - startTime;
      System.out.println("N1: " + n1 + " -> " + HumansUtil.humanTimeNanos(endTime));

      startTime = System.nanoTime();
      final String n2 = encodeBaseN(n, BASE_58);
      endTime = System.nanoTime() - startTime;
      System.out.println("N2: " + n2 + " -> " + HumansUtil.humanTimeNanos(endTime));
    }
  }
}
