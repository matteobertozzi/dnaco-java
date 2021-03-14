package tech.dnaco.collections.maps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import tech.dnaco.collections.maps.LongToObjectMap.Entry;

public class TestLongToObjectMap {
  @Test
  public void testCrud() {
    final ArrayList<TestObject> data = new ArrayList<>();
    for (int i = 0; i < 500; ++i) {
      data.add(new TestObject(i, 1));
    }

    final LongToObjectMap<TestObject> map = new LongToObjectMap<>();

    // Insert data
    assertEquals(0, map.size());
    assertEquals(true, map.isEmpty());
    for (final TestObject obj: data) {
      assertEquals(null, map.put(obj.key, obj));
    }
    assertEquals(data.size(), map.size());
    assertEquals(data.isEmpty(), map.isEmpty());

    // validate data
    for (final TestObject obj: data) {
      assertEquals(true, map.containsKey(obj.key));
      assertEquals(obj, map.get(obj.key));
    }
    assertEquals(false, map.containsKey(10000));
    assertEquals(null, map.get(10000));

    // test remove
    for (final TestObject obj: data) {
      assertEquals(obj, map.remove(obj.key));
      assertEquals(false, map.containsKey(obj.key));
      assertEquals(null, map.get(obj.key));
    }
    assertEquals(0, map.size());
    assertEquals(true, map.isEmpty());
  }

  @Test
  public void testPutIfAbsent() {
    final TestObject[] data = new TestObject[3];
    data[0] = new TestObject(1, 1);
    data[1] = new TestObject(2, 1);
    data[2] = new TestObject(3, 1);

    final LongToObjectMap<TestObject> map = new LongToObjectMap<>();

    // insert
    for (int i = 0; i < data.length; ++i) {
      assertEquals(null, map.put(data[i].key, data[i]));
    }

    // validate
    for (int i = 0; i < data.length; ++i) {
      assertEquals(true, map.containsKey(data[i].key));
      assertEquals(data[i], map.get(data[i].key));
    }

    // replace
    for (int i = 0; i < data.length; ++i) {
      final TestObject oldValue = data[i];
      data[i] = new TestObject(oldValue.key, oldValue.count + 1);
      assertEquals(oldValue, map.put(data[i].key, data[i]));
      assertEquals(data[i], map.get(data[i].key));
    }

    // validate
    for (int i = 0; i < data.length; ++i) {
      assertEquals(true, map.containsKey(data[i].key));
      assertEquals(data[i], map.get(data[i].key));
    }

    // put if absent
    for (int i = 0; i < data.length; ++i) {
      final TestObject oldValue = data[i];
      final TestObject newValue = new TestObject(oldValue.key, oldValue.count + 1);
      assertEquals(oldValue, map.putIfAbsent(data[i].key, newValue));
      assertEquals(oldValue, map.get(data[i].key));
      assertEquals(oldValue, data[i]);
    }

    // validate
    for (int i = 0; i < data.length; ++i) {
      assertEquals(true, map.containsKey(data[i].key));
      assertEquals(data[i], map.get(data[i].key));
    }
  }

  @Test
  public void testComputeIfAbsent() {
    final LongToObjectMap<TestObject> map = new LongToObjectMap<>();

    final TestObject obj1 = map.computeIfAbsent(1, k -> new TestObject(1, 1));
    final TestObject obj1b = map.computeIfAbsent(1, k -> new TestObject(1, 1));
    assertTrue(obj1 == obj1b);

    final TestObject obj2 = map.computeIfAbsent(2, k -> new TestObject(2, 1));
    assertTrue(obj1 != obj2);

    final TestObject[] objs = map.values();
    assertEquals(2, objs.length);

    System.out.println(Arrays.toString(objs));
    assertTrue(objs[0] == obj1 || objs[0] == obj2);
    assertTrue(objs[1] == obj1 || objs[1] == obj2);
  }

  @Test
  public void testIterator() {
    final ArrayList<TestObject> data = new ArrayList<>();
    for (int i = 0; i < 20; ++i) {
      data.add(new TestObject(i, 1));
    }

    final LongToObjectMap<TestObject> map = new LongToObjectMap<>();
    for (final TestObject obj: data) {
      map.put(obj.key, obj);
    }
    assertEquals(false, map.isEmpty());

    int count = 0;
    for (final LongToObjectMap.Entry<TestObject> entry: map) {
      final TestObject obj = data.get((int)entry.getKey());
      assertEquals(obj.key, entry.getKey());
      assertEquals(obj, entry.getValue());
      count++;
    }
    assertEquals(count, map.size());

    // test keySet
    final long[] keys = map.keySet();
    assertEquals(data.size(), keys.length);
    for (int i = 0; i < keys.length; ++i) {
      final TestObject obj = data.get((int)keys[i]);
      assertEquals(keys[i], obj.key);
    }

    // test clear
    map.clear();
    assertEquals(0, map.size());
    assertEquals(true, map.isEmpty());
    for (final LongToObjectMap.Entry<TestObject> entry: map) {
      fail("unexpected item " + entry.getKey());
    }
  }

  private static class TestObject {
    private final long key;
    private final int count;

    private TestObject(final long key, final int count) {
      this.key = key;
      this.count = count;
    }

    @Override
    public String toString() {
      return key + ":" + count;
    }
  }

  @Test
  public void testSimple() throws Exception {
    final LongToObjectMap<TestObject> hsDbConnection = new LongToObjectMap<>();
    for (int i = 0; i < 128; ++i) {
      hsDbConnection.put(i, new TestObject(i, i));

      final BitSet bitmap = new BitSet(i + 1);
      for (final Entry<TestObject> entry: hsDbConnection) {
        bitmap.set((int)entry.getKey());
      }
      assertIsAllSet(bitmap);
    }
  }

  private static void assertIsAllSet(final BitSet bitmap) {
    for (int i = 0; i < bitmap.length(); ++i) {
      assertEquals(true, bitmap.get(i), i + " not set");
    }
  }
}
