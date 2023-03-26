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

package tech.dnaco.bytes.encoding;

import java.math.BigInteger;

public final class IntUtil {
  private IntUtil() {
    // no-op
  }

  public static int getWidth(final int v) {
    return 32 - Integer.numberOfLeadingZeros(v);
  }

  public static int getWidth(final long v) {
    return 64 - Long.numberOfLeadingZeros(v);
  }

  public static int size(final int v) {
    return Math.max(1, (getWidth(v) + 7) >> 3);
  }

  public static int size(final long v) {
    return Math.max(1, (getWidth(v) + 7) >> 3);
  }

  public static int unsignedVarLongSize(long v) {
    int result = 0;
    do {
      result++;
      v >>>= 7;
    } while (v != 0);
    return result;
  }

  public static int zigZagEncode(final int n) {
    return (n << 1) ^ (n >> 31);
  }

  public static long zigZagEncode(final long n) {
    return (n << 1) ^ (n >> 63);
  }

  public static int zigZagDecode(final int n) {
    return (n >> 1) ^ (-(n & 1));
  }

  public static long zigZagDecode(final long n) {
    return (n >> 1) ^ (-(n & 1));
  }

  public static BigInteger toUnsignedBigInteger(final long v) {
    if (v >= 0L) return BigInteger.valueOf(v);

    // return (upper << 32) + lower
    final long upper = (v >>> 32) & 0xffffffffL;
    final long lower = v & 0xffffffffL;
    return (BigInteger.valueOf(upper)).shiftLeft(32).add(BigInteger.valueOf(lower));
  }
}
