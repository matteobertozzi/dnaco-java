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
import java.util.HashMap;

public final class BaseN {
  private static final char[] BASE_35 = "0123456789ABCDEFGHIJKLMNOPQRTUVWXYZ".toCharArray();
  private static final char[] BASE_62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  private BaseN() {
    // no-op
  }

  public static String encodeBase35(final long value) {
    return encodeBaseN(value, BASE_35);
  }

  public static String encodeBase35(final BigInteger value) {
    return encodeBaseN(value, BASE_35);
  }

  public static long decodeBase35(final String value) {
    return decodeBaseN(value, BASE_35).longValue();
  }

  public static BigInteger decodeBigBase35(final String value) {
    return decodeBaseN(value, BASE_35);
  }

  public static String encodeBase62(final long value) {
    return encodeBaseN(value, BASE_62);
  }

  public static String encodeBase62(final BigInteger value) {
    return encodeBaseN(value, BASE_62);
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
    final StringBuilder result = new StringBuilder();
    final int base = dictionary.length;
    long remaining = value;
    for (int exponent = 1; true; ++exponent) {
      final long a = Math.round(Math.pow(base, exponent));
      final long b = remaining % a;
      final long c = Math.round(Math.pow(base, exponent - 1));
      final int d = (int) (b / c);

      result.append(dictionary[d]);
      if ((remaining = remaining - b) == 0) {
        break;
      }
    }
    return result.reverse().toString();
  }

  public static String encodeBaseN(final BigInteger value, final char[] dictionary) {
    final StringBuilder result = new StringBuilder();
    final BigInteger base = BigInteger.valueOf(dictionary.length);
    BigInteger remaining = value;
    for (int exponent = 1; true; ++exponent) {
      final BigInteger a = base.pow(exponent);
      final BigInteger b = remaining.mod(a);
      final BigInteger c = base.pow(exponent - 1);
      final BigInteger d = b.divide(c);

      result.append(dictionary[d.intValue()]);
      remaining = remaining.subtract(b);
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
}
