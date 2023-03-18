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

package tech.dnaco.bytes.encoding;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.LongValue;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.RandData;

public class TestRowKey {
  @Test
  public void testIntKey() {
    byte[] prevKey = new byte[0];
    for (int i = 0, n = 1 << 12; i < n; ++i) {
      final byte[] key = RowKey.newKeyBuilder().addInt64(i).drain();
      Assertions.assertTrue(BytesUtil.compare(prevKey, key) < 0,
        "prev=" + BytesUtil.toHexString(prevKey) + ", key=" + BytesUtil.toHexString(key));
      prevKey = key;
    }
  }

  @Test
  public void testLast() {
    final byte[] key = RowKey.newKeyBuilder().add("x").addInt64(1).drain();
    Assertions.assertEquals("780000000100010001000100010001000101", BytesUtil.toHexString(key));
    final ByteArraySlice last = RowKey.lastKeyComponent(key);
    Assertions.assertEquals(8, last.length());
    Assertions.assertEquals("0000000000000001", BytesUtil.toHexString(last.buffer()));
    Assertions.assertEquals(1, IntDecoder.BIG_ENDIAN.readFixed64(last.buffer(), 0));
  }

  @Test
  public void testLast3() {
    final byte[] key2 = RowKey.newKeyBuilder().add("x").add("y").drain();
    final ByteArraySlice last2 = RowKey.lastKeyComponent(key2);
    Assertions.assertEquals(1, last2.length());
    Assertions.assertArrayEquals(new byte[] { 'y' }, last2.buffer());

    final byte[] key3 = RowKey.newKeyBuilder().add("x").add(new byte[] { 1 }).drain();
    final ByteArraySlice last3 = RowKey.lastKeyComponent(key3);
    Assertions.assertEquals(1, last3.length());
    Assertions.assertArrayEquals(new byte[] { 1 }, last3.buffer());
  }


  @Test
  public void testLast2() {
    final byte[] key = RowKey.newKeyBuilder().addInt32(0).addInt64(1).drain();
    // 0001|0001|0001|0001| 0000 |0001|0001|0001|0001|0001|0001|0001|01
    Assertions.assertEquals("00010001000100010000000100010001000100010001000101", BytesUtil.toHexString(key));
    final ByteArraySlice last = RowKey.lastKeyComponent(key);
    Assertions.assertEquals(8, last.length());
    Assertions.assertEquals("0000000000000001", BytesUtil.toHexString(last.buffer()));
    Assertions.assertEquals(1, IntDecoder.BIG_ENDIAN.readFixed64(last.buffer(), 0));
  }

  @Test
  public void testKeyWithout() {
    //final byte[] key = RowKey.newKeyBuilder().addInt32(0).addInt64(1).drain();
    final byte[] key1 = RowKey.newKeyBuilder().add("aaa").add("bbb").drain();
    assertEquals("6161610000626262", BytesUtil.toHexString(key1));
    final ByteArraySlice keyP1 = RowKey.keyWithoutLastComponent(key1);
    assertEquals("616161", BytesUtil.toHexString(keyP1.buffer(), keyP1.offset(), keyP1.length()));


    final byte[] key2 = RowKey.newKeyBuilder().addInt32(0).addInt64(1).drain();
    assertEquals("00010001000100010000000100010001000100010001000101", BytesUtil.toHexString(key2));
    final ByteArraySlice keyP2 = RowKey.keyWithoutLastComponent(key2);
    assertEquals("0001000100010001", BytesUtil.toHexString(keyP2.buffer(), keyP2.offset(), keyP2.length()));
  }

  @Test
  public void testDecode() {
    final byte[] xkey = RowKey.newKeyBuilder()
      .addInt8(1)
      .addInt16(2)
      .addInt24(3)
      .addInt32(4)
      .addInt40(5)
      .addInt48(6)
      .addInt56(7)
      .addInt64(8)
      .addInt(9)
      .add("foo")
      .drain();

    final String[] expectedHexPart = new String[] {
      "01", "0002", "000003", "00000004", "0000000005", "000000000006",
      "00000000000007", "0000000000000008", "09", "666f6f"
    };
    final LongValue partIndex = new LongValue();
    RowKey.decodeKey(xkey, k -> {
      final String hexPart = BytesUtil.toHexString(k.rawBuffer(), k.offset(), k.length());
      Assertions.assertEquals(expectedHexPart[(int) partIndex.incrementAndGet() - 1], hexPart);
    });

    final RowKey key = new RowKey(xkey);
    Assertions.assertEquals(1, key.getInt8(0));
    Assertions.assertEquals(2, key.getInt16(1));
    Assertions.assertEquals(3, key.getInt24(2));
    Assertions.assertEquals(4, key.getInt32(3));
    Assertions.assertEquals(5, key.getInt40(4));
    Assertions.assertEquals(6, key.getInt48(5));
    Assertions.assertEquals(7, key.getInt56(6));
    Assertions.assertEquals(8, key.getInt64(7));
    Assertions.assertEquals(9, key.getLong(8));
    Assertions.assertEquals(9, key.getInt(8));
    Assertions.assertEquals("foo", key.getString(9));
  }

  public record CompositeKey1 (String a, int b, ByteArraySlice c) implements Comparable<CompositeKey1> {
    @Override
    public int compareTo(final CompositeKey1 o) {
      int cmp;
      cmp = StringUtil.compare(a, o.a);
      if (cmp != 0) return cmp;
      cmp = Integer.compare(b, o.b);
      if (cmp != 0) return cmp;
      cmp = c.compareTo(o.c);
      if (cmp != 0) return cmp;
      return cmp;
    }
  }

  public record CompositeKey2 (int a, ByteArraySlice b, String c) implements Comparable<CompositeKey2> {
    @Override
    public int compareTo(final CompositeKey2 o) {
      int cmp;
      cmp = Integer.compare(a, o.a);
      if (cmp != 0) return cmp;
      cmp = b.compareTo(o.b);
      if (cmp != 0) return cmp;
      cmp = StringUtil.compare(c, o.c);
      if (cmp != 0) return cmp;
      return cmp;
    }
  }

  public record CompositeKey3 (ByteArraySlice a, String b, int c) implements Comparable<CompositeKey3> {
    @Override
    public int compareTo(final CompositeKey3 o) {
      int cmp = a.compareTo(o.a);
      if (cmp != 0) return cmp;
      cmp = StringUtil.compare(b, o.b);
      if (cmp != 0) return cmp;
      return Integer.compare(c, o.c);
    }
  }

  @Test
  public void testRandom() {
    final ArrayList<CompositeKey1> k1Record = new ArrayList<>();
    final ArrayList<CompositeKey2> k2Record = new ArrayList<>();
    final ArrayList<CompositeKey3> k3Record = new ArrayList<>();
    final ArrayList<ByteArraySlice> k1Raw = new ArrayList<>();
    final ArrayList<ByteArraySlice> k2Raw = new ArrayList<>();
    final ArrayList<ByteArraySlice> k3Raw = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      final int strLen = RandData.generateInt(0, 16);
      final String strValue = RandData.generateAlphaNumericString(strLen);
      for (int j = 0; j < 3; ++j) {
        final int bytesLen = RandData.generateInt(0, 32);
        final byte[] bytesValue = RandData.generateBytes(bytesLen);
        for (int k = 0; k < 3; ++k) {
          final int intValue = RandData.generateInt(0, Integer.MAX_VALUE);
          k1Record.add(new CompositeKey1(strValue, intValue, new ByteArraySlice(bytesValue)));
          k2Record.add(new CompositeKey2(intValue, new ByteArraySlice(bytesValue), strValue));
          k3Record.add(new CompositeKey3(new ByteArraySlice(bytesValue), strValue, intValue));
          k1Raw.add(RowKey.newKeyBuilder().add(strValue).addInt32(intValue).add(bytesValue).slice());
          k2Raw.add(RowKey.newKeyBuilder().addInt32(intValue).add(bytesValue).add(strValue).slice());
          k3Raw.add(RowKey.newKeyBuilder().add(bytesValue).add(strValue).addInt32(intValue).slice());
        }
      }
    }

    Collections.sort(k1Record);
    Collections.sort(k2Record);
    Collections.sort(k3Record);
    Collections.sort(k1Raw);
    Collections.sort(k2Raw);
    Collections.sort(k3Raw);

    for (int i = 0, len = k1Record.size(); i < len; ++i) {
      final CompositeKey1 rec = k1Record.get(i);
      final RowKey key = new RowKey(k1Raw.get(i).buffer());
      System.out.println(rec);
      System.out.println(k1Raw.get(i));
      assertEquals(rec.a(), key.getString(0));
      assertEquals(rec.b(), key.getInt32(1));
      assertEquals(rec.c(), key.get(2));
    }

    for (int i = 0, len = k2Record.size(); i < len; ++i) {
      final CompositeKey2 rec = k2Record.get(i);
      final RowKey key = new RowKey(k2Raw.get(i).buffer());
      assertEquals(rec.a(), key.getInt32(0));
      assertEquals(rec.b(), key.get(1));
      assertEquals(rec.c(), key.getString(2));
    }

    for (int i = 0, len = k3Record.size(); i < len; ++i) {
      final CompositeKey3 rec = k3Record.get(i);
      final RowKey key = new RowKey(k3Raw.get(i).buffer());
      assertEquals(rec.a(), key.get(0));
      assertEquals(rec.b(), key.getString(1));
      assertEquals(rec.c(), key.getInt32(2));
    }
  }
}
