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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.util.RandData;

public class TestBaseN {
  @Test
  public void testLongBaseN() {
    Assertions.assertEquals("bUKpk", BaseN.encodeBase58(123456789));
    Assertions.assertEquals(123456789, BaseN.decodeBase58("bUKpk"));

    Assertions.assertEquals("JPwcyDCgEup", BaseN.encodeBase58(-1L));
    Assertions.assertEquals(-1L, BaseN.decodeBase58("JPwcyDCgEup"));

    Assertions.assertEquals("JPwcyDCgEup", BaseN.encodeBase58(IntUtil.toUnsignedBigInteger(-1L)));
    Assertions.assertEquals(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE), BaseN.decodeBigBase58("JPwcyDCgEup"));
  }

  @Test
  public void testFoo() {
    final byte[] data = new byte[] { (byte)0x8f };
    final String base62 = BaseN.encodeBase62(data);
    final BigInteger decoded = BaseN.decodeBigBase62(base62);
    System.out.println(new BigInteger(data));
    System.out.println(decoded);
    System.out.println(BytesUtil.toHexString(data));
    System.out.println(BytesUtil.toHexString(decoded.negate().toByteArray()));
  }

  @Test
  public void testRandomBytes() {
    for (int i = 0; i < 10; ++i) {
      final byte[] data = RandData.generateBytes((int) Math.round(Math.random() * 512));
      System.out.println(i + " -> " + BytesUtil.toHexString(data));
      final String base62 = BaseN.encodeBase62(data);
      final BigInteger v = BaseN.decodeBigBase62(base62);
      Assertions.assertEquals(BytesUtil.toHexString(data), BytesUtil.toHexString(v.toByteArray()));
      Assertions.assertArrayEquals(data, v.toByteArray());
    }
  }
}
