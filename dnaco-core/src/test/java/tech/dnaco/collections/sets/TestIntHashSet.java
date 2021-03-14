package tech.dnaco.collections.sets;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIntHashSet {
  @Test
  public void testEmpty() {
    final IntHashSet sset = new IntHashSet();
    Assertions.assertEquals(0, sset.size());
    Assertions.assertTrue(sset.isEmpty());
    Assertions.assertFalse(sset.isNotEmpty());
    Assertions.assertFalse(sset.contains(1));
    Assertions.assertFalse(sset.contains(2));
  }

  @Test
  public void testSimple() {
    final IntHashSet sset = new IntHashSet();
    Assertions.assertTrue(sset.add(10));
    Assertions.assertTrue(sset.contains(10));
    Assertions.assertFalse(sset.add(10));
    Assertions.assertTrue(sset.contains(10));
    Assertions.assertTrue(sset.remove(10));
    Assertions.assertFalse(sset.contains(10));
    Assertions.assertFalse(sset.remove(10));
    Assertions.assertFalse(sset.contains(10));
    Assertions.assertTrue(sset.add(10));
    Assertions.assertTrue(sset.contains(10));
    sset.clear();
    Assertions.assertFalse(sset.contains(10));
  }

  @Test
  public void testResize() {
    final long seed = Math.round(Math.random() * 1_000_000);
    final Random random = new Random(seed);

    final HashSet<Integer> values = new HashSet<>();

    final IntHashSet sset = new IntHashSet();
    for (int i = 0; i < 2000; ++i) {
      final int v = random.nextInt(1000);
      Assertions.assertEquals(values.add(v), sset.add(v));
      Assertions.assertTrue(sset.contains(v));
      for (final Integer existingValue: values) {
        Assertions.assertTrue(sset.contains(existingValue));
      }
    }

    final HashSet<Integer> removed = new HashSet<>();
    final Iterator<Integer> itValues = values.iterator();
    while (itValues.hasNext()) {
      final int v = itValues.next();
      removed.add(v);
      itValues.remove();

      Assertions.assertTrue(sset.remove(v));
      Assertions.assertFalse(sset.remove(v));
      for (final Integer existingValue: values) {
        Assertions.assertTrue(sset.contains(existingValue));
      }
      for (final Integer removedValue: removed) {
        Assertions.assertFalse(sset.contains(removedValue));
      }
    }
    Assertions.assertEquals(0, sset.size());
  }

  @Test
  public void test() {
    final int TEST_OUT_RANGE = 20_000;
    final int TEST_IN_RANGE = 10_000;

    final HashSet<Integer> hset = new HashSet<>();
    final IntHashSet ihset = new IntHashSet();
    for (int i = 0; i < TEST_IN_RANGE; ++i) {
      hset.add(i);
      ihset.add(i);
    }

    for (final Integer x: hset) {
      assertEquals(true, ihset.contains(x.intValue()));
    }
    for (int i = TEST_IN_RANGE + 1; i < TEST_OUT_RANGE; ++i) {
      assertEquals(false, ihset.contains(i));
    }

    for (int i = 0; i < TEST_IN_RANGE; i += 2) {
      hset.remove(i);
      ihset.remove(i);
    }
    for (final Integer x: hset) {
      assertEquals(true, ihset.contains(x.intValue()));
    }
    for (int i = TEST_IN_RANGE + 1; i < TEST_OUT_RANGE; ++i) {
      assertEquals(false, ihset.contains(i));
    }
    Assertions.assertEquals(TEST_IN_RANGE / 2, ihset.size());
  }
}
