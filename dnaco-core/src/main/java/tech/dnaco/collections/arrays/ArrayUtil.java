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

package tech.dnaco.collections.arrays;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public final class ArrayUtil {
  public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  private ArrayUtil() {
    // no-op
  }

  // ================================================================================
  //  PUBLIC unchecked cast related
  // ================================================================================
  @SuppressWarnings("unchecked")
  public static <T> T getItemAt(final Object[] array, final int index) {
    return (T) array[index];
  }

  @SuppressWarnings("unchecked")
  public static <T> T[] newArray(final int size, final Class<?> clazz) {
    return (T[]) Array.newInstance(clazz, size);
  }

  @SuppressWarnings("unchecked")
  public static <T> T[] newArray(final int size) {
    return (T[]) new Object[size];
  }

  @SuppressWarnings("unchecked")
  public static <T> T[] emptyArray() {
    return (T[]) EMPTY_OBJECT_ARRAY;
  }

  // ================================================================================
  //  PUBLIC Array length related
  // ================================================================================
  public static int length(final byte[] input) {
    return input == null ? 0 : input.length;
  }

  public static int length(final int[] input) {
    return input == null ? 0 : input.length;
  }

  public static int length(final long[] input) {
    return input == null ? 0 : input.length;
  }

  public static int length(final float[] input) {
    return input == null ? 0 : input.length;
  }

  public static int length(final double[] input) {
    return input == null ? 0 : input.length;
  }

  public static <T> int length(final T[] input) {
    return input == null ? 0 : input.length;
  }

  public static boolean isEmpty(final byte[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isEmpty(final char[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isEmpty(final int[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isEmpty(final long[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isEmpty(final float[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isEmpty(final double[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isEmpty(final Object[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isNotEmpty(final byte[] input) {
    return (input != null) && (input.length != 0);
  }

  public static boolean isNotEmpty(final char[] input) {
    return (input != null) && (input.length != 0);
  }

  public static boolean isNotEmpty(final int[] input) {
    return (input != null) && (input.length != 0);
  }

  public static boolean isNotEmpty(final float[] input) {
    return (input != null) && (input.length != 0);
  }

  public static boolean isNotEmpty(final double[] input) {
    return (input != null) && (input.length != 0);
  }

  public static boolean isNotEmpty(final long[] input) {
    return (input != null) && (input.length != 0);
  }

  public static boolean isNotEmpty(final Object[] input) {
    return (input != null) && (input.length != 0);
  }

  // ================================================================================
  //  PUBLIC Array Consumer Interfaces
  // ================================================================================
  public interface ArrayConsumer<T> {
    void accept(T[] buf, int off, int len);
  }

  public interface ByteArrayConsumer {
    void accept(byte[] buf, int off, int len);
  }

  public interface IntArrayConsumer {
    void accept(int[] buf, int off, int len);
  }

  public interface LongArrayConsumer {
    void accept(long[] buf, int off, int len);
  }

  public interface FloatArrayConsumer {
    void accept(float[] buf, int off, int len);
  }

  public interface DoubleArrayConsumer {
    void accept(double[] buf, int off, int len);
  }

  // ================================================================================
  //  PUBLIC Array resize helpers
  // ================================================================================
  public static byte[] newIfNotAtSize(final byte[] buf, final int size) {
    if (buf != null && buf.length >= size) return buf;
    return new byte[size];
  }

  public static int[] newIfNotAtSize(final int[] buf, final int size) {
    if (buf != null && buf.length >= size) return buf;
    return new int[size];
  }

  public static long[] newIfNotAtSize(final long[] buf, final int size) {
    if (buf != null && buf.length >= size) return buf;
    return new long[size];
  }

  // ================================================================================
  //  PUBLIC Array copy helpers
  // ================================================================================
  public static byte[] copyIfNotAtSize(final byte[] buf, final int off, final int len) {
    if (off == 0 && buf.length == len) return buf;
    return Arrays.copyOfRange(buf, off, off + len);
  }

  public static int[] copyIfNotAtSize(final int[] buf, final int off, final int len) {
    if (off == 0 && buf.length == len) return buf;
    return Arrays.copyOfRange(buf, off, off + len);
  }

  public static long[] copyIfNotAtSize(final long[] buf, final int off, final int len) {
    if (off == 0 && buf.length == len) return buf;
    return Arrays.copyOfRange(buf, off, off + len);
  }

  public static <T> T[] copyIfNotAtSize(final T[] buf, final int off, final int len) {
    if (off == 0 && buf.length == len) return buf;
    return Arrays.copyOfRange(buf, off, off + len);
  }

  // ================================================================================
  //  PUBLIC Array indexOf helpers
  // ================================================================================
  public static int indexOf(final byte[] buf, final int off, final int len, final byte value) {
    for (int i = 0; i < len; ++i) {
      if (buf[off + i] == value) {
        return i;
      }
    }
    return -1;
  }

  public static int indexOf(final int[] buf, final int off, final int len, final int value) {
    for (int i = 0; i < len; ++i) {
      if (buf[off + i] == value) {
        return i;
      }
    }
    return -1;
  }

  public static int indexOf(final long[] buf, final int off, final int len, final long value) {
    for (int i = 0; i < len; ++i) {
      if (buf[off + i] == value) {
        return i;
      }
    }
    return -1;
  }

  public static int indexOf(final Object[] buf, final int off, final int len, final Object value) {
    for (int i = 0; i < len; ++i) {
      if (Objects.equals(buf[off + i], value)) {
        return i;
      }
    }
    return -1;
  }

  // ================================================================================
  //  PUBLIC Array insert helpers
  // ================================================================================
  public static void insert(final int[] array, final int off, final int len,
      final int index, final int value) {
    System.arraycopy(array, off + index, array, off + index + 1, len - index);
    array[index] = value;
  }

  public static void insert(final long[] array, final int off, final int len,
      final int index, final long value) {
    System.arraycopy(array, off + index, array, off + index + 1, len - index);
    array[index] = value;
  }

  public static <T> void insert(final T[] array, final int off, final int len,
      final int index, final T value) {
    System.arraycopy(array, off + index, array, off + index + 1, len - index);
    array[index] = value;
  }

  // ================================================================================
  //  PUBLIC Array sorted insert related
  // ================================================================================
  public static void sortedInsert(final byte[] array, final int fromIndex, final int toIndex, final byte value) {
    int index = Arrays.binarySearch(array, fromIndex, toIndex, value);
    if (index < 0) index = (-index) - 1;
    System.arraycopy(array, index, array, index + 1, toIndex - index);
    array[index] = value;
  }

  public static void sortedInsert(final int[] array, final int fromIndex, final int toIndex, final int value) {
    int index = Arrays.binarySearch(array, fromIndex, toIndex, value);
    if (index < 0) index = (-index) - 1;
    System.arraycopy(array, index, array, index + 1, toIndex - index);
    array[index] = value;
  }

  public static void sortedInsert(final long[] array, final int fromIndex, final int toIndex, final long value) {
    int index = Arrays.binarySearch(array, fromIndex, toIndex, value);
    if (index < 0) index = (-index) - 1;
    System.arraycopy(array, index, array, index + 1, toIndex - index);
    array[index] = value;
  }

  public static <T extends Comparable<T>> void sortedInsert(final T[] array, final int fromIndex, final int toIndex, final T value) {
    int index = Arrays.binarySearch(array, fromIndex, toIndex, value);
    if (index < 0) index = (-index) - 1;
    System.arraycopy(array, index, array, index + 1, toIndex - index);
    array[index] = value;
  }

  public static <T> void sortedInsert(final T[] array, final int fromIndex, final int toIndex, final T value, final Comparator<T> comparator) {
    int index = Arrays.binarySearch(array, fromIndex, toIndex, value, comparator);
    if (index < 0) index = (-index) - 1;
    System.arraycopy(array, index, array, index + 1, toIndex - index);
    array[index] = value;
  }

  // ================================================================================
  //  PUBLIC Array Remove related
  // ================================================================================
  public static void removeElementWithShift(final int[] arr, final int index){
    System.arraycopy(arr, index + 1, arr, index, arr.length - (1 + index));
  }

  public static void removeElementWithShift(final long[] arr, final int index){
    System.arraycopy(arr, index + 1, arr, index, arr.length - (1 + index));
  }

  public static <T> void removeElementWithShift(final T[] arr, final int index){
    System.arraycopy(arr, index + 1, arr, index, arr.length - (1 + index));
  }

  // ================================================================================
  //  PUBLIC Array Clear related
  // ================================================================================
  public static void clear(final byte[] data) {
    if (isEmpty(data)) return;
    Arrays.fill(data, Byte.MAX_VALUE);
  }

  public static void clear(final char[] data) {
    if (isEmpty(data)) return;
    Arrays.fill(data, Character.MAX_VALUE);
  }

  public static void clear(final int[] data) {
    if (isEmpty(data)) return;
    Arrays.fill(data, Integer.MAX_VALUE);
  }

  public static void clear(final long[] data) {
    if (isEmpty(data)) return;
    Arrays.fill(data, Long.MAX_VALUE);
  }

  public static void clear(final Object[] data) {
    if (isEmpty(data)) return;
    Arrays.fill(data, null);
  }

  // ================================================================================
  //  PUBLIC Array Item Swap related
  // ================================================================================
  public static void swap(final byte[] values, final int aIndex, final int bIndex) {
    final byte tmp = values[aIndex];
    values[aIndex] = values[bIndex];
    values[bIndex] = tmp;
  }

  public static void swap(final int[] values, final int aIndex, final int bIndex) {
    final int tmp = values[aIndex];
    values[aIndex] = values[bIndex];
    values[bIndex] = tmp;
  }

  public static void swap(final long[] values, final int aIndex, final int bIndex) {
    final long tmp = values[aIndex];
    values[aIndex] = values[bIndex];
    values[bIndex] = tmp;
  }

  public static void swap(final float[] values, final int aIndex, final int bIndex) {
    final float tmp = values[aIndex];
    values[aIndex] = values[bIndex];
    values[bIndex] = tmp;
  }

  public static void swap(final double[] values, final int aIndex, final int bIndex) {
    final double tmp = values[aIndex];
    values[aIndex] = values[bIndex];
    values[bIndex] = tmp;
  }

  public static <T> void swap(final T[] values, final int aIndex, final int bIndex) {
    final T tmp = values[aIndex];
    values[aIndex] = values[bIndex];
    values[bIndex] = tmp;
  }

  // ================================================================================
  //  PUBLIC Array concat() helpers
  // ================================================================================
  public static String[] concat(final String[]... arrays) {
    int length = 0;
    for (int i = 0; i < arrays.length; ++i) {
      length += length(arrays[i]);
    }

    int index = 0;
    final String[] values = new String[length];
    for (int i = 0; i < arrays.length; ++i) {
      final int len = length(arrays[i]);
      if (len == 0) continue;

      System.arraycopy(arrays[i], 0, values, index, len);
      index += len;
    }
    return values;
  }

  // ================================================================================
  //  PUBLIC Array toString() helpers
  // ================================================================================
  public static String toString(final int[] buf, final int off, final int len) {
    if (buf == null) return "null";
    if (len == 0 || off >= buf.length) return "[]";
    return toString(new StringBuilder(len * 5), buf, off, len).toString();
  }

  public static StringBuilder toString(final StringBuilder sb,
      final int[] buf, final int off, final int len) {
    if (buf == null) return sb.append("null");
    if (len == 0 || off >= buf.length) return sb.append("[]");

    sb.append('[');
    for (int i = 0; i < len; ++i) {
      if (i > 0) sb.append(", ");
      sb.append(buf[off + i]);
    }
    sb.append(']');
    return sb;
  }

  public static String toString(final long[] buf, final int off, final int len) {
    if (buf == null) return "null";
    if (len == 0 || off >= buf.length) return "[]";
    return toString(new StringBuilder(len * 8), buf, off, len).toString();
  }

  public static StringBuilder toString(final StringBuilder sb,
      final long[] buf, final int off, final int len) {
    if (buf == null) return sb.append("null");
    if (len == 0 || off >= buf.length) return sb.append("[]");

    sb.append('[');
    for (int i = 0; i < len; ++i) {
      if (i > 0) sb.append(", ");
      sb.append(buf[off + i]);
    }
    sb.append(']');
    return sb;
  }

  public static <T> String toString(final T[] buf, final int off, final int len) {
    if (buf == null) return "null";
    if (len == 0 || off >= buf.length) return "[]";
    return toString(new StringBuilder(len * 8), buf, off, len).toString();
  }

  public static <T> StringBuilder toString(final StringBuilder sb,
      final T[] buf, final int off, final int len) {
    if (buf == null) return sb.append("null");
    if (len == 0 || off >= buf.length) return sb.append("[]");

    sb.append('[');
    for (int i = 0; i < len; ++i) {
      if (i > 0) sb.append(", ");
      sb.append(buf[off + i]);
    }
    sb.append(']');
    return sb;
  }

  public static String toStringWithoutNull(final Object[] a) {
    if (a == null) return "null";
    if (a.length == 0) return "[]";

    final StringBuilder b = new StringBuilder(a.length * 4);
    b.append('[');
    for (int i = 0, count = 0; i < a.length; i++) {
      if (a[i] == null) continue;

      if (count++ > 0) b.append(", ");
      b.append(a[i]);
    }
    return b.append(']').toString();
  }
}
