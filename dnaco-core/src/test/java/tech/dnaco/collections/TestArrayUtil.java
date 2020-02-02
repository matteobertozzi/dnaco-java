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

package tech.dnaco.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import tech.dnaco.collections.ArrayUtil;

public class TestArrayUtil {
  @Test
  public void testIndexOf() {
    final int[] iArray = new int[] { 1, 2, 3 };

    assertEquals(0, ArrayUtil.indexOf(iArray, 0, 1, 1));
    assertEquals(0, ArrayUtil.indexOf(iArray, 1, 2, 2));
    assertEquals(2, ArrayUtil.indexOf(iArray, 0, 3, 3));

    assertEquals(-1, ArrayUtil.indexOf(iArray, 0, 3, 57));
    assertEquals(-1, ArrayUtil.indexOf(iArray, 1, 2, 1));
    assertEquals(-1, ArrayUtil.indexOf(iArray, 2, 1, 2));
    assertEquals(-1, ArrayUtil.indexOf(iArray, 0, 2, 3));
  }

  @Test
  public void testSwap() {
    final String[] sArray = new String[] { "aaa", "bbb", "ccc" };
    ArrayUtil.swap(sArray, 1, 2);
    assertEquals("aaa", sArray[0]);
    assertEquals("ccc", sArray[1]);
    assertEquals("bbb", sArray[2]);

    final long[] lArray = new long[] { 0x40000000001L, 0x40000000002L, 0x40000000003L };
    ArrayUtil.swap(lArray, 1, 2);
    assertEquals(0x40000000001L, lArray[0]);
    assertEquals(0x40000000003L, lArray[1]);
    assertEquals(0x40000000002L, lArray[2]);

    final int[] iArray = new int[] { 1, 2, 3 };
    ArrayUtil.swap(iArray, 1, 2);
    assertEquals(1, iArray[0]);
    assertEquals(3, iArray[1]);
    assertEquals(2, iArray[2]);
  }

  @Test
  public void testToString() {
    assertEquals("null", ArrayUtil.toString((int[])null, 0, 10));
    assertEquals("null", ArrayUtil.toString((long[])null, 0, 10));
    assertEquals("null", ArrayUtil.toString((String[])null, 0, 10));

    final String[] sArray = new String[] { "aaa", "bbb", "ccc" };
    final long[] lArray = new long[] { 0x40000000001L, 0x40000000002L, 0x40000000003L };
    final int[] iArray = new int[] { 1, 2, 3 };

    assertEquals("[]", ArrayUtil.toString(iArray, 0, 0));
    assertEquals("[]", ArrayUtil.toString(lArray, 0, 0));
    assertEquals("[]", ArrayUtil.toString(sArray, 0, 0));

    assertEquals("[1]", ArrayUtil.toString(iArray, 0, 1));
    assertEquals("[4398046511105]", ArrayUtil.toString(lArray, 0, 1));
    assertEquals("[aaa]", ArrayUtil.toString(sArray, 0, 1));

    assertEquals("[2, 3]", ArrayUtil.toString(iArray, 1, 2));
    assertEquals("[4398046511106, 4398046511107]", ArrayUtil.toString(lArray, 1, 2));
    assertEquals("[bbb, ccc]", ArrayUtil.toString(sArray, 1, 2));
  }

  @Test
  public void testMerge() {
    final long[] a = new long[] { 20, 30, 60, 80 };
    final long[] b = new long[] { 10, 30, 50, 60, 70, 90 };

    System.out.println(Arrays.toString(ArraySortUtil.merge(a, b)));
    System.out.println(Arrays.toString(ArraySortUtil.mergeAndSquash(a, b)));
  }
}