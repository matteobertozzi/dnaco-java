package tech.dnaco.collections.paged;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import tech.dnaco.collections.ArrayUtil;

public class TestPagedIntArray {
  @Test
  public void testSimpleAdd() {
    final int totalItems = 8 * 128;
    final PagedIntArray array = new PagedIntArray(8);
    for (int i = 0; i < totalItems; ++i) {
      array.add(i);
      assertEquals(1 + i, array.size());
    }
    for (int i = 0; i < totalItems; ++i) {
      assertEquals(i, array.get(i));
    }
    assertEquals(totalItems, array.size());
  }

  @Test
  public void testSimpleSet() {
    final PagedIntArray array = new PagedIntArray(8);
    for (int i = 0; i < 64; ++i) array.add(i);

    for (int i = 0; i < 64; ++i) {
      array.set(i, 1000 + i);
    }

    for (int i = 0; i < 64; ++i) {
      assertEquals(1000 + i, array.get(i));
    }
    assertEquals(64, array.size());
  }

  @Test
  public void testClear() {
    final PagedIntArray array = new PagedIntArray(8);
    for (int i = 0; i < 64; ++i) array.add(i);
    assertEquals(64, array.size());

    // try to call clear a bunch of times
    for (int i = 0; i < 2; ++i) {
      array.clear();
      assertEquals(0, array.size());
      array.clear(true);
      assertEquals(0, array.size());
    }

    array.add(new int[] { 1, 2, 3, 4 }, 1, 2);
    assertEquals(2, array.size());
    assertEquals(2, array.get(0));
    assertEquals(3, array.get(1));
  }

  @Test
  public void testForEach() {
    final PagedIntArray array = new PagedIntArray(16);
    for (int i = 0; i < 64; ++i) array.add(i);
    assertEquals(64, array.size());

    AtomicLong counter = new AtomicLong(0);
    array.forEach((buf, off, len) -> counter.addAndGet(ArrayUtil.sum(buf, off, len)));
    assertEquals(2016, counter.get());

    counter.set(0);
    array.forEach(2, 4, (buf, off, len) -> {
      System.out.println(ArrayUtil.toString(buf, off, len));
      counter.addAndGet(ArrayUtil.sum(buf, off, len));
    });
    assertEquals(14, counter.get());

    counter.set(0);
    array.forEach(2, 64 - 3, (buf, off, len) -> {
      System.out.println(ArrayUtil.toString(buf, off, len));
      counter.addAndGet(ArrayUtil.sum(buf, off, len));
    });
    assertEquals(1952, counter.get());

    counter.set(0);
    array.forEach(18, 64 - 18 - 18, (buf, off, len) -> {
      System.out.println(ArrayUtil.toString(buf, off, len));
      counter.addAndGet(ArrayUtil.sum(buf, off, len));
    });
    assertEquals(882, counter.get());
  }
}
