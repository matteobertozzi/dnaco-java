package tech.dnaco.collections.sets;

import java.util.Arrays;
import java.util.function.LongConsumer;

import tech.dnaco.collections.Hashing;
import tech.dnaco.util.BitUtil;

public class LongHashSet {
  private static final long SPECIAL_VALUE = Long.MIN_VALUE;

  private long[] table;
  private boolean containsSpecialValue;
  private int count;

  public LongHashSet() {
    this(0);
  }

  public LongHashSet(final int initialSize) {
    table = new long[BitUtil.nextPow2(initialSize)];
    Arrays.fill(table, SPECIAL_VALUE);
    containsSpecialValue = false;
    count = 0;
  }

  public static LongHashSet fromArray(final long[] values) {
    final LongHashSet hashSet = new LongHashSet(Math.round(values.length * 1.5f));
    hashSet.addAll(values);
    return hashSet;
  }

  public int size() {
    return count;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  public boolean isNotEmpty() {
    return count != 0;
  }

  public void clear() {
    Arrays.fill(table, SPECIAL_VALUE);
    containsSpecialValue = false;
    count = 0;
  }

  public boolean contains(final long value) {
    return value != SPECIAL_VALUE ? getIndex(value) >= 0 : containsSpecialValue;
  }

  private int getIndex(final long value) {
    if (table.length == 0) return -1;

    final int mask = table.length - 1;
    int index = Hashing.hash(value, mask);
    while (table[index] != SPECIAL_VALUE) {
      if (table[index] == value) {
        return index;
      }
      index = ++index & mask;
    }
    return -1;
  }

  public boolean add(final long value) {
    if (value == SPECIAL_VALUE) {
      if (containsSpecialValue) {
        return false;
      }
      count++;
      containsSpecialValue = true;
      return true;
    }

    if ((count + 2) > table.length) {
      resize();
    }

    final boolean added = add(table, value);
    if (added) count++;
    return added;
  }

  public int addAll(final long[] values) {
    int count = 0;
    for (int i = 0; i < values.length; ++i) {
      count += add(values[i]) ? 1 : 0;
    }
    return count;
  }

  private static boolean add(final long[] table, final long value) {
    final int mask = table.length - 1;
    int index = Hashing.hash(value, mask);
    //System.out.println(table.length + ":" + Arrays.toString(table));
    while (table[index] != SPECIAL_VALUE) {
      if (table[index] == value) {
        return false;
      }
      index = ++index & mask;
    }
    table[index] = value;
    return true;
  }

  public boolean remove(final long value) {
    if (value == SPECIAL_VALUE) {
      if (containsSpecialValue) {
        count--;
        return true;
      }
      return false;
    }

    final int index = getIndex(value);
    if (index < 0) return false;

    table[index] = SPECIAL_VALUE;
    if (--count < (table.length / 3)) {
      compact();
    }
    return true;
  }

  private void resize() {
    final int newCapacity = (table.length != 0) ? table.length << 1 : 8;
    if (newCapacity < 0) {
      throw new IllegalStateException("Set too big size=" + table.length);
    }
    resize(newCapacity);
  }

  private void compact() {
    final int newCapacity = table.length >> 1;
    if (count >= newCapacity) {
      throw new IllegalStateException("resize too short newCapacity=" + newCapacity + ", itemCount=" + count);
    }
    resize(newCapacity);
  }

  private void resize(final int newSize) {
    final long[] newTable = new long[newSize];
    Arrays.fill(newTable, SPECIAL_VALUE);

    for (int i = 0; i < table.length; ++i) {
      if (table[i] != SPECIAL_VALUE) {
        add(newTable, table[i]);
      }
    }
    table = newTable;
  }

  public long[] keySet() {
    int index = 0;
    final long[] keySet = new long[count];
    for (int i = 0; i < table.length; ++i) {
      if (table[i] != SPECIAL_VALUE) {
        keySet[index++] = table[i];
      }
    }
    if (containsSpecialValue) {
      keySet[index] = SPECIAL_VALUE;
    }
    return keySet;
  }

  public void forEach(final LongConsumer consumer) {
    for (int i = 0; i < table.length; ++i) {
      if (table[i] != SPECIAL_VALUE) {
        consumer.accept(table[i]);
      }
    }
    if (containsSpecialValue) {
      consumer.accept(SPECIAL_VALUE);
    }
  }

  @Override
  public String toString() {
    if (count == 0) return "{}";

    int index = 0;
    final StringBuilder text = new StringBuilder();
    text.append("{");
    for (int i = 0; i < table.length; ++i) {
      if (table[i] == SPECIAL_VALUE) continue;

      if (index++ > 0) text.append(", ");
      text.append(table[i]);
    }
    if (containsSpecialValue) {
      if (index > 0) text.append(", ");
      text.append(SPECIAL_VALUE);
    }
    text.append("}");
    return text.toString();
  }
}
