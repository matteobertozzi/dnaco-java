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

public final class BaseN {
  public static final char[] BASE_36_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
  public static final char[] BASE_52_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
  public static final char[] BASE_58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".toCharArray();
  public static final char[] BASE_62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

  private BaseN() {
    // no-op
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
}
