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
import java.util.HashMap;

import tech.dnaco.bytes.BytesUtil;

public final class BaseN {
  public static final char[] BASE_35_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRTUVWXYZ".toCharArray();
  public static final char[] BASE_36_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
  public static final char[] BASE_52_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
  public static final char[] BASE_58_ALPHABET = "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ".toCharArray();
  public static final char[] BASE_62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
  public static final char[] BASE_90_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&()*+,-./:;<=>?@[]^_{|}~".toCharArray();

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

  public static byte[] decodeHumanBase32(final String data) {
    return Base32.decode(data);
  }

  // ================================================================================
  //  Base36 encoder/decoder
  // ================================================================================
  public static String encodeBase36(final long value) {
    return encodeBaseN(value, BASE_36_ALPHABET);
  }

  public static String encodeBase36(final byte[] value) {
    return encodeBase58(new BigInteger(value));
  }

  public static String encodeBase36(final BigInteger value) {
    return encodeBaseN(value, BASE_36_ALPHABET);
  }

  public static long decodeBase36(final String value) {
    return decodeBigBase36(value).longValueExact();
  }

  public static BigInteger decodeBigBase36(final String value) {
    return decodeBaseN(value, BASE_36_ALPHABET);
  }

  // ================================================================================
  //  Base52 encoder/decoder
  // ================================================================================
  public static String encodeBase52(final long value) {
    return encodeBaseN(value, BASE_52_ALPHABET);
  }

  public static String encodeBase52(final byte[] value) {
    return encodeBase52(new BigInteger(value));
  }

  public static String encodeBase52(final BigInteger value) {
    return encodeBaseN(value, BASE_52_ALPHABET);
  }

  public static long decodeBase52(final String value) {
    return decodeBigBase52(value).longValueExact();
  }

  public static BigInteger decodeBigBase52(final String value) {
    return decodeBaseN(value, BASE_52_ALPHABET);
  }

  // ================================================================================
  //  Base58 encoder/decoder
  // ================================================================================
  private static final long[] BASE_58_POW = new long[] {
    1, 58, 3364, 195112, 11316496, 656356768, 38068692544L, 2207984167552L,
    128063081718016L, 7427658739644928L, 430804206899405824L
  };
  private static final int[] BASE_58_DECODER = new int[128];
  static {
    for (int i = 0; i < BASE_58_ALPHABET.length; ++i) {
      BASE_58_DECODER[BASE_58_ALPHABET[i]] = i;
    }
  }

  public static String encodeBase58(final long value) {
    final StringBuilder builder = new StringBuilder();
    encodeBase58(builder, value);
    return builder.reverse().toString();
  }

  public static void encodeBase58(final StringBuilder builder, final long value) {
    long remaining = value;
    do {
      final int d = (int) Long.remainderUnsigned(remaining, BASE_58_ALPHABET.length);
      remaining = Long.divideUnsigned(remaining, BASE_58_ALPHABET.length);
      builder.append(BASE_58_ALPHABET[d]);
    } while (remaining != 0);
  }

  public static String encodeBase58(final byte[] value) {
    return encodeBase58(new BigInteger(value));
  }

  public static String encodeBase58(final BigInteger value) {
    return encodeBaseN(value, BASE_58_ALPHABET);
  }

  public static long decodeBase58(final String value) {
    return decodeBase58(value, 0, value.length());
  }

  public static long decodeBase58(final String data, final int offset, final int length) {
    long v = 0;
    for (int i = 0; i < length; ++i) {
      v += BASE_58_POW[i] * BASE_58_DECODER[data.charAt(offset + (length - 1) - i)];
    }
    return v;
  }

  public static BigInteger decodeBigBase58(final String value) {
    return decodeBaseN(value, BASE_58_ALPHABET);
  }

  // ================================================================================
  //  Base62 encoder/decoder
  // ================================================================================
  public static String encodeBase62(final long value) {
    return encodeBaseN(value, BASE_62_ALPHABET);
  }

  public static String encodeBase62(final byte[] value) {
    return encodeBase62(new BigInteger(value));
  }

  public static String encodeBase62(final BigInteger value) {
    return encodeBaseN(value, BASE_62_ALPHABET);
  }

  public static long decodeBase62(final String value) {
    return decodeBigBase62(value).longValueExact();
  }

  public static BigInteger decodeBigBase62(final String value) {
    return decodeBaseN(value, BASE_62_ALPHABET);
  }

  // ================================================================================
  //  BaseN encoder/decoder
  // ================================================================================
  public static String encodeBaseN(final long value, final char[] dictionary) {
    final int base = dictionary.length;
    long remaining = value;

    final StringBuilder result = new StringBuilder();
    do {
      final int d = (int) Long.remainderUnsigned(remaining, base);
      remaining = Long.divideUnsigned(remaining, base);

      result.append(dictionary[d]);
    } while (remaining != 0);
    return result.reverse().toString();
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

  public static void main(final String[] args) {
    final byte[] b = Base32.decode("fn9dm7txjwk17w5ar5gzaqtqd5dp3y9yf7gbphg3hjr7c51gio7o");
    System.out.println(BytesUtil.toHexString(b));
  }
}
