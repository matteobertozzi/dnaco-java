package tech.dnaco.collections;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestObjectHashSet {
  @Test
  public void testCapacity() {
    final ObjectHashSet<String> zero = new ObjectHashSet<>();
    Assertions.assertTrue(zero.isEmpty());
    Assertions.assertEquals(0, zero.size());
    Assertions.assertEquals(1, zero.capacity());

    final ObjectHashSet<String> three = new ObjectHashSet<>(3);
    Assertions.assertTrue(three.isEmpty());
    Assertions.assertEquals(0, three.size());
    Assertions.assertEquals(4, three.capacity());

    final ObjectHashSet<String> ten = new ObjectHashSet<>(10);
    Assertions.assertTrue(ten.isEmpty());
    Assertions.assertEquals(0, ten.size());
    Assertions.assertEquals(16, ten.capacity());
  }

  @Test
  public void testEmpty() {
    final ObjectHashSet<String> sset = new ObjectHashSet<>();
    Assertions.assertEquals(0, sset.size());
    Assertions.assertEquals(1, sset.capacity());
    Assertions.assertTrue(sset.isEmpty());
    Assertions.assertFalse(sset.isNotEmpty());
    Assertions.assertFalse(sset.contains("foo"));
    Assertions.assertFalse(sset.contains("bar"));
  }

  @Test
  public void testSimple() {
    final ObjectHashSet<Integer> sset = new ObjectHashSet<>();
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

    final ObjectHashSet<Integer> sset = new ObjectHashSet<>();
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
