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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

public class TestArraySortUtil {
  @Test
  public void testMerge() {
    final long[] a = new long[] { 20, 30, 60, 80 };
    final long[] b = new long[] { 10, 30, 50, 60, 70, 90 };

    assertArrayEquals(new long[] {10, 20, 30, 30, 50, 60, 60, 70, 80, 90}, ArraySortUtil.merge(a, b));
    assertArrayEquals(new long[] {10, 20, 30, 50, 60, 70, 80, 90}, ArraySortUtil.mergeAndSquash(a, b));
  }

  @Test
  public void testSort() {
    final String[] data = new String[] { "bbb", "ccc", "aaa", "ddd" };
    final int[] index = new int[] { 0, 1, 2, 3 };

    ArraySortUtil.sort(index, 0, index.length, (idx, a, b) -> data[idx[a]].compareTo(data[idx[b]]));
    assertArrayEquals(new int[] {2, 0, 1, 3}, index);
  }
}