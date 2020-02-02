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

public final class ArraySortUtil {
  private ArraySortUtil() {
    // no-op
  }

  // ================================================================================
  //  Merge related
  // ================================================================================
  public static long[] merge(final long[] a, final long[] b) {
    return merge(a, 0, a.length, b, 0, b.length);
  }

  public static long[] merge(final long[] a, final int aOff, final int aLen,
      final long[] b, final int bOff, final int bLen) {
    final long[] merged = new long[aLen + bLen];
    int i = 0, j = 0, count = 0;
    while (i < aLen && j < bLen) {
      if (a[aOff + i] < b[bOff + j]) {
        merged[count++] = a[aOff + i++];
      } else {
        merged[count++] = b[bOff + j++];
      }
    }
    System.arraycopy(a, aOff + i, merged, count, aLen - i);
    count += aLen - i;
    System.arraycopy(b, bOff + j, merged, count, bLen - j);
    return merged;
  }

  public static long[] mergeAndSquash(final long[] a, final long[] b) {
    return mergeAndSquash(a, 0, a.length, b, 0, b.length);
  }

  public static long[] mergeAndSquash(final long[] a, final int aOff, final int aLen,
      final long[] b, final int bOff, final int bLen) {
    int i = 0, j = 0, count = 0;
    while (i < aLen && j < bLen) {
      final int cmp = Long.compare(a[aOff + i], b[bOff + j]);
      i += (cmp <= 0) ? 1 : 0;
      j += (cmp >= 0) ? 1 : 0;
      count++;
    }

    final long[] merged = new long[count + (aLen - i) + (bLen - j)];
    i = j = count = 0;
    while (i < aLen && j < bLen) {
      final long aValue = a[aOff + i];
      final long bValue = b[bOff + j];
      final int cmp = Long.compare(aValue, bValue);
      merged[count++] = (cmp < 0) ? aValue : bValue;
      i += (cmp <= 0) ? 1 : 0;
      j += (cmp >= 0) ? 1 : 0;
    }
    System.arraycopy(a, aOff + i, merged, count, aLen - i);
    count += aLen - i;
    System.arraycopy(b, bOff + j, merged, count, bLen - j);
    return merged;
  }

  // ================================================================================
  //  Int Sort related
  // ================================================================================
  public interface IntArrayComparator {
    int compare(int[] array, int aIndex, int bIndex);
  }

  public static final IntArrayComparator INT_ARRAY_COMPARATOR = new IntArrayComparator() {
    @Override
    public int compare(final int[] array, final int aIndex, final int bIndex) {
      return Integer.compare(array[aIndex], array[bIndex]);
    }
  };

  public static void sort(final int[] buf, final int off, final int len, final IntArrayComparator comparator) {
    int i = (len / 2 - 1);

    // heapify
    for (; i >= 0; --i) {
      int c = i * 2 + 1;
      int r = i;
      while (c < len) {
        if (c < len - 1 && comparator.compare(buf, off + c, off + c + 1) < 0)
          c += 1;
        if (comparator.compare(buf, off + r, off + c) >= 0)
          break;
        ArrayUtil.swap(buf, off + r, off + c);
        r = c;
        c = r * 2 + 1;
      }
    }

    // sort
    for (i = len - 1; i > 0; --i) {
      int c = 1;
      int r = 0;
      ArrayUtil.swap(buf, off + 0, off + i);
      while (c < i) {
        if (c < i - 1 && comparator.compare(buf, off + c, off + c + 1) < 0)
          c += 1;
        if (comparator.compare(buf, off + r, off + c) >= 0)
          break;
        ArrayUtil.swap(buf, off + r, off + c);
        r = c;
        c = r * 2 + 1;
      }
    }
  }
}