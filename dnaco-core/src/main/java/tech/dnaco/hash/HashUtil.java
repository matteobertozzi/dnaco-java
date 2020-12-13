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

package tech.dnaco.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import tech.dnaco.bytes.BytesUtil;

public final class HashUtil {
  private HashUtil() {
    // no-op
  }

  public static int hash(final int value, final int mask) {
    final int hash = value * 31;
    return Math.abs(hash & mask);
  }

  public static int hash(final long value, final int mask) {
    long hash = value * 31;
    hash = (int) hash ^ (int) (hash >>> 32);
    return Math.abs((int) hash & mask);
  }

  public static int hash32(final String value) {
    return value != null ? Math.abs(value.hashCode() & 0x7fffffff) : 0;
  }

  public static int triple32(int x) {
    x ^= x >>> 17;
    x *= 0xed5ad4bb;
    x ^= x >>> 11;
    x *= 0xac4c1b51;
    x ^= x >>> 15;
    x *= 0x31848bab;
    x ^= x >>> 14;
    return x;
  }

  // inverse
  public static int triple32_r(int x) {
    x ^= x >>> 14 ^ x >>> 28;
    x *= 0x32b21703L;
    x ^= x >>> 15 ^ x >>> 30;
    x *= 0x469e0db1L;
    x ^= x >>> 11 ^ x >>> 22;
    x *= 0x79a85073L;
    x ^= x >>> 17;
    return x;
  }

  public static byte[] hash(final String algo, final byte[] buf) {
    return hash(algo, buf, 0, BytesUtil.length(buf));
  }

  public static byte[] hash(final String algo, final byte[] buf, final int off, final int len) {
    try {
      final MessageDigest digest = MessageDigest.getInstance(algo);
      digest.update(buf, off, len);
      return digest.digest();
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
