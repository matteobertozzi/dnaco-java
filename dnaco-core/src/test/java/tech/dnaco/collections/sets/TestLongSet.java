package tech.dnaco.collections.sets;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLongSet {
  @Test
  public void testEmpty() {
    final LongHashSet sset = new LongHashSet();
    Assertions.assertEquals(0, sset.size());
    Assertions.assertTrue(sset.isEmpty());
    Assertions.assertFalse(sset.isNotEmpty());
    Assertions.assertFalse(sset.contains(1));
    Assertions.assertFalse(sset.contains(2));
  }

  @Test
  public void testSimple() {
    final LongHashSet sset = new LongHashSet();
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

    final LongHashSet sset = new LongHashSet();
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
}
