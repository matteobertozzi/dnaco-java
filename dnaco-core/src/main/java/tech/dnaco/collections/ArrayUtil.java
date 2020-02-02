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

import java.util.Arrays;

public final class ArrayUtil {
  private ArrayUtil() {
    // no-op
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

  public static <T> int length(final T[] input) {
    return input == null ? 0 : input.length;
  }

  public static boolean isEmpty(final byte[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isEmpty(final int[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isEmpty(final long[] input) {
    return (input == null) || (input.length == 0);
  }

  public static <T> boolean isEmpty(final T[] input) {
    return (input == null) || (input.length == 0);
  }

  public static boolean isNotEmpty(final byte[] input) {
    return (input != null) && input.length > 0;
  }

  public static boolean isNotEmpty(final int[] input) {
    return (input != null) && input.length > 0;
  }

  public static boolean isNotEmpty(final long[] input) {
    return (input != null) && input.length > 0;
  }

  public static <T> boolean isNotEmpty(final T[] input) {
    return (input != null) && input.length > 0;
  }

  // ================================================================================
  //  PUBLIC Array sum helpers
  // ================================================================================
  public static long sum(final int[] buf, final int off, final int len) {
    if (buf == null || len == 0) return 0;

    long sum = 0;
    for (int i = 0; i < len; ++i) {
      sum += buf[off + i];
    }
    return sum;
  }

  public static long sum(final long[] buf, final int off, final int len) {
    if (buf == null || len == 0) return 0;

    long sum = 0;
    for (int i = 0; i < len; ++i) {
      sum += buf[off + i];
    }
    return sum;
  }

  // ================================================================================
  //  PUBLIC Array toString() helpers
  // ================================================================================
  public static String toString(final int[] buf, final int off, final int len) {
    if (buf == null) return "null";
    if (len == 0) return "[]";
    return toString(new StringBuilder(len * 5), buf, off, len).toString();
  }

  public static StringBuilder toString(final StringBuilder sb,
      final int[] buf, final int off, final int len) {
    if (buf == null) return sb.append("null");
    if (len == 0) return sb.append("[]");

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
    if (len == 0) return "[]";
    return toString(new StringBuilder(len * 8), buf, off, len).toString();
  }

  public static StringBuilder toString(final StringBuilder sb,
      final long[] buf, final int off, final int len) {
    if (buf == null) return sb.append("null");
    if (len == 0) return sb.append("[]");

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
    if (len == 0) return "[]";
    return toString(new StringBuilder(len * 8), buf, off, len).toString();
  }

  public static <T> StringBuilder toString(final StringBuilder sb,
      final T[] buf, final int off, final int len) {
    if (buf == null) return sb.append("null");
    if (len == 0) return sb.append("[]");

    sb.append('[');
    for (int i = 0; i < len; ++i) {
      if (i > 0) sb.append(", ");
      sb.append(buf[off + i]);
    }
    sb.append(']');
    return sb;
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
  //  PUBLIC Array insert helpers
  // ================================================================================
  public static void insert(final int[] array, final int off, final int len,
      final int index, final int value) {
    System.arraycopy(array, off + index, array, off + index + 1, len - index);
    array[index] = value;
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

  // ================================================================================
  //  PUBLIC Array Swap helpers
  // ================================================================================
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

  public static <T> void swap(final T[] values, final int aIndex, final int bIndex) {
    final T tmp = values[aIndex];
    values[aIndex] = values[bIndex];
    values[bIndex] = tmp;
  }
}