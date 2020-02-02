package tech.dnaco.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.Test;

public class TestHashIndexedArray {
  @Test
  public void testData() {
    final Random rand = new Random();
    for (int z = 0; z < 10000; ++z) {
      final String[] keys = new String[rand.nextInt(20)];
      for (int i = 0; i < keys.length; ++i) {
        keys[i] = String.valueOf(rand.nextLong());
      }

      final HashIndexedArray<String> indexedArray = new HashIndexedArray<>(keys);
      assertEquals(keys.length, indexedArray.size());
      for (int i = 0; i < keys.length; ++i) {
        assertEquals(true, indexedArray.contains(keys[i]));
        final int index = indexedArray.getIndex(keys[i]);
        assertEquals(i, index);
        assertEquals(keys[i], keys[index]);
      }

      for (int i = 0; i < 100; ++i) {
        assertEquals(false, indexedArray.contains(UUID.randomUUID().toString()));
      }
    }
  }
}
