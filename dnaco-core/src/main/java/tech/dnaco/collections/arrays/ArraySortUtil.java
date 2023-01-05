package tech.dnaco.collections.arrays;

import java.util.Arrays;

public final class ArraySortUtil {
  private ArraySortUtil() {
    // no-op
  }

  public static int binarySearch(final int[] a, final int off, final int len, final int stride, final int key) {
    int low = off;
    int high = off + len - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int midVal = a[mid * stride];

      if (midVal < key) {
        low = mid + 1;
      } else if (midVal > key) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1);  // key not found.
  }

  public static int binarySearch(final int off, final int len, final ArrayIndexKeyComparator comparator) {
    int low = off;
    int high = off + len - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int cmp = comparator.compareKeyWith(mid);

      if (cmp > 0) {
        low = mid + 1;
      } else if (cmp < 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1);  // key not found.
  }

  @FunctionalInterface
  public interface ArrayIndexKeyComparator {
    int compareKeyWith(int index);
  }

  @FunctionalInterface
  public interface ArrayIndexComparator {
    int compare(int aIndex, int bIndex);
  }

  @FunctionalInterface
  public interface ArrayIndexSwapper {
    void swap(int aIndex, int bIndex);
  }

  public static void sort(final int off, final int len,
      final ArrayIndexComparator comparator, final ArrayIndexSwapper swapper) {
    int i = (len / 2 - 1);

    // heapify
    for (; i >= 0; --i) {
      int c = i * 2 + 1;
      int r = i;
      while (c < len) {
        if (c < len - 1 && comparator.compare(off + c, off + c + 1) < 0) {
          c += 1;
        }
        if (comparator.compare(off + r, off + c) >= 0) {
          break;
        }
        swapper.swap(off + r, off + c);
        r = c;
        c = r * 2 + 1;
      }
    }

    // sort
    for (i = len - 1; i > 0; --i) {
      int c = 1;
      int r = 0;
      swapper.swap(off, off + i);
      while (c < i) {
        if (c < i - 1 && comparator.compare(off + c, off + c + 1) < 0) {
          c += 1;
        }
        if (comparator.compare(off + r, off + c) >= 0) {
          break;
        }
        swapper.swap(off + r, off + c);
        r = c;
        c = r * 2 + 1;
      }
    }
  }

  public static void main(final String[] args) {
    if (true) {
      final int[] index = new int[] { 1, 10, 2, 20, 3, 30, 4, 40 };
      sort(0, index.length / 2, (a, b) -> Long.compare(index[b * 2], index[a * 2]), (a, b) -> {
        final int aIndex = a * 2;
        final int bIndex = b * 2;
        ArrayUtil.swap(index, aIndex, bIndex);
        ArrayUtil.swap(index, aIndex + 1, bIndex + 1);
      });
      System.out.println(Arrays.toString(index));
    }
  }
}
