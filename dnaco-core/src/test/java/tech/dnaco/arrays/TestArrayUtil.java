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

package tech.dnaco.arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.collections.arrays.ArrayUtil;

public class TestArrayUtil {
  // ================================================================================
  //  Array length related
  // ================================================================================
  @Test
  public void testNullArrayLength() {
    Assertions.assertTrue(ArrayUtil.isEmpty((byte[])null));
    Assertions.assertTrue(ArrayUtil.isEmpty((int[])null));
    Assertions.assertTrue(ArrayUtil.isEmpty((long[])null));
    Assertions.assertTrue(ArrayUtil.isEmpty((float[])null));
    Assertions.assertTrue(ArrayUtil.isEmpty((double[])null));
    Assertions.assertTrue(ArrayUtil.isEmpty((String[])null));

    Assertions.assertFalse(ArrayUtil.isNotEmpty((byte[])null));
    Assertions.assertFalse(ArrayUtil.isNotEmpty((int[])null));
    Assertions.assertFalse(ArrayUtil.isNotEmpty((long[])null));
    Assertions.assertFalse(ArrayUtil.isNotEmpty((float[])null));
    Assertions.assertFalse(ArrayUtil.isNotEmpty((double[])null));
    Assertions.assertFalse(ArrayUtil.isNotEmpty((String[])null));

    Assertions.assertEquals(0, ArrayUtil.length((byte[])null));
    Assertions.assertEquals(0, ArrayUtil.length((int[])null));
    Assertions.assertEquals(0, ArrayUtil.length((long[])null));
    Assertions.assertEquals(0, ArrayUtil.length((float[])null));
    Assertions.assertEquals(0, ArrayUtil.length((double[])null));
    Assertions.assertEquals(0, ArrayUtil.length((String[])null));
  }

  @Test
  public void testEmptyArrayLength() {
    Assertions.assertTrue(ArrayUtil.isEmpty(new byte[0]));
    Assertions.assertTrue(ArrayUtil.isEmpty(new int[0]));
    Assertions.assertTrue(ArrayUtil.isEmpty(new long[0]));
    Assertions.assertTrue(ArrayUtil.isEmpty(new float[0]));
    Assertions.assertTrue(ArrayUtil.isEmpty(new double[0]));
    Assertions.assertTrue(ArrayUtil.isEmpty(new String[0]));

    Assertions.assertFalse(ArrayUtil.isNotEmpty(new byte[0]));
    Assertions.assertFalse(ArrayUtil.isNotEmpty(new int[0]));
    Assertions.assertFalse(ArrayUtil.isNotEmpty(new long[0]));
    Assertions.assertFalse(ArrayUtil.isNotEmpty(new float[0]));
    Assertions.assertFalse(ArrayUtil.isNotEmpty(new double[0]));
    Assertions.assertFalse(ArrayUtil.isNotEmpty(new String[0]));

    Assertions.assertEquals(0, ArrayUtil.length(new byte[0]));
    Assertions.assertEquals(0, ArrayUtil.length(new int[0]));
    Assertions.assertEquals(0, ArrayUtil.length(new long[0]));
    Assertions.assertEquals(0, ArrayUtil.length(new float[0]));
    Assertions.assertEquals(0, ArrayUtil.length(new double[0]));
    Assertions.assertEquals(0, ArrayUtil.length(new String[0]));
  }

  // ================================================================================
  //  Array indexOf helpers
  // ================================================================================
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

  // ================================================================================
  //  Array sorted insert related
  // ================================================================================
  @Test
  public void testSortedInsert() {
    final int[] array = new int[16];
    final int[] toInsert = new int[] { 4, 2, 8, 3, 1, 7, 0, 5, 2 };
    for (int i = 0; i < toInsert.length; ++i) {
      ArrayUtil.sortedInsert(array, 2, 2 + i, toInsert[i]);
    }
    Assertions.assertArrayEquals(new int[] {0, 0, 0, 1, 2, 2, 3, 4, 5, 7, 8, 0, 0, 0, 0, 0}, array);
  }

  // ================================================================================
  //  Array Item Swap related
  // ================================================================================
  @Test
  public void testSwap() {
    final String[] sArray = new String[] { "aaa", "bbb", "ccc" };
    ArrayUtil.swap(sArray, 1, 2);
    Assertions.assertEquals("aaa", sArray[0]);
    Assertions.assertEquals("ccc", sArray[1]);
    Assertions.assertEquals("bbb", sArray[2]);

    final long[] lArray = new long[] { 0x40000000001L, 0x40000000002L, 0x40000000003L };
    ArrayUtil.swap(lArray, 1, 2);
    Assertions.assertEquals(0x40000000001L, lArray[0]);
    Assertions.assertEquals(0x40000000003L, lArray[1]);
    Assertions.assertEquals(0x40000000002L, lArray[2]);

    final int[] iArray = new int[] { 1, 2, 3 };
    ArrayUtil.swap(iArray, 1, 2);
    Assertions.assertEquals(1, iArray[0]);
    Assertions.assertEquals(3, iArray[1]);
    Assertions.assertEquals(2, iArray[2]);
  }
}
