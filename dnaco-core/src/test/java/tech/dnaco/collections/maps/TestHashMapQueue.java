package tech.dnaco.collections.maps;

import java.util.ArrayList;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHashMapQueue {

  @Test
  public void testCrud() {
    final TestObject[] data = new TestObject[64];
    for (int i = 0; i < data.length; ++i) {
      data[i] = new TestObject(i, 1);
    }

    final HashMapQueue<String, TestObject> queue = new HashMapQueue<>();

    // test push
    Assertions.assertEquals(true, queue.isEmpty());
    Assertions.assertEquals(0, queue.size());
    for (final TestObject obj: data) {
      Assertions.assertEquals(false, queue.containsKey(obj.key));
      Assertions.assertEquals(null, queue.get(obj.key));

      Assertions.assertEquals(null, queue.push(obj.key, obj));
      Assertions.assertEquals(true, queue.containsKey(obj.key));
      Assertions.assertEquals(obj, queue.get(obj.key));
    }
    Assertions.assertEquals(false, queue.isEmpty());
    Assertions.assertEquals(data.length, queue.size());

    // test peek/pop
    for (int i = 0; i < data.length; ++i) {
      Assertions.assertEquals(false, queue.isEmpty());
      Assertions.assertEquals(data.length - i, queue.size());

      final TestObject obj = data[i];
      Assertions.assertEquals(obj.key, queue.peekKey());
      Assertions.assertEquals(obj, queue.peekValue());

      final Map.Entry<String, TestObject> peekEntry = queue.peek();
      Assertions.assertEquals(obj.key, peekEntry.getKey());
      Assertions.assertEquals(obj, peekEntry.getValue());

      final Map.Entry<String, TestObject> entry = queue.pop();
      Assertions.assertEquals(peekEntry, entry);
      Assertions.assertEquals(obj.key, entry.getKey());
      Assertions.assertEquals(obj, entry.getValue());
    }
    Assertions.assertEquals(true, queue.isEmpty());
    Assertions.assertEquals(0, queue.size());
  }

  @Test
  public void testPutIfAbsent() {
    final TestObject[] data = new TestObject[3];
    data[0] = new TestObject(1, 1);
    data[1] = new TestObject(2, 1);
    data[2] = new TestObject(3, 1);

    final HashMapQueue<String, TestObject> queue = new HashMapQueue<>();

    // insert
    for (int i = 0; i < data.length; ++i) {
      Assertions.assertEquals(null, queue.push(data[i].key, data[i]));
    }

    // validate
    for (int i = 0; i < data.length; ++i) {
      Assertions.assertEquals(true, queue.containsKey(data[i].key));
      Assertions.assertEquals(data[i], queue.get(data[i].key));
    }

    // replace
    for (int i = 0; i < data.length; ++i) {
      final TestObject oldValue = data[i];
      data[i] = new TestObject(oldValue.key, oldValue.count + 1);
      Assertions.assertEquals(oldValue, queue.push(data[i].key, data[i]));
      Assertions.assertEquals(data[i], queue.get(data[i].key));
    }

    // validate
    for (int i = 0; i < data.length; ++i) {
      Assertions.assertEquals(true, queue.containsKey(data[i].key));
      Assertions.assertEquals(data[i], queue.get(data[i].key));
    }

    // put if absent
    for (int i = 0; i < data.length; ++i) {
      final TestObject oldValue = data[i];
      final TestObject newValue = new TestObject(oldValue.key, oldValue.count + 1);
      Assertions.assertEquals(oldValue, queue.pushIfAbsent(data[i].key, newValue));
      Assertions.assertEquals(oldValue, queue.get(data[i].key));
      Assertions.assertEquals(oldValue, data[i]);
    }

    // validate
    for (int i = 0; i < data.length; ++i) {
      Assertions.assertEquals(true, queue.containsKey(data[i].key));
      Assertions.assertEquals(data[i], queue.get(data[i].key));
    }
  }

  @Test
  public void testIterator() {
    final ArrayList<TestObject> data = new ArrayList<>();
    for (int i = 0; i < 20; ++i) {
      data.add(new TestObject(i, 1));
    }

    final HashMapQueue<String, TestObject> queue = new HashMapQueue<>();
    for (final TestObject obj: data) {
      queue.push(obj.key, obj);
    }
    Assertions.assertEquals(false, queue.isEmpty());

    int count = 0;
    for (final Map.Entry<String, TestObject> entry: queue) {
      final int index = Integer.parseInt(entry.getKey());
      Assertions.assertEquals(count, index);

      final TestObject obj = data.get(index);
      Assertions.assertEquals(obj.key, entry.getKey());
      Assertions.assertEquals(obj, entry.getValue());
      count++;
    }
    Assertions.assertEquals(count, queue.size());

    // test clear
    queue.clear();
    Assertions.assertEquals(0, queue.size());
    Assertions.assertEquals(true, queue.isEmpty());
    for (final Map.Entry<String, TestObject> entry: queue) {
      Assertions.fail("unexpected item " + entry.getKey());
    }
  }

  private static class TestObject {
    private final String key;
    private final int count;

    private TestObject(final int key, final int count) {
      this(Integer.toString(key), count);
    }

    private TestObject(final String key, final int count) {
      this.key = key;
      this.count = count;
    }

    @Override
    public String toString() {
      return key + ':' + count;
    }
  }
}
