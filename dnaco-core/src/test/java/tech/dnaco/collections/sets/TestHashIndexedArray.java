package tech.dnaco.collections.sets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHashIndexedArray {
  @Test
  public void testSimple() {
    final Integer[] in = new Integer[] { 1, 2, 3, 4, 5};
    final Integer[] out = new Integer[] { 11, 12, 13, 14, 15 };
    final HashIndexedArray<Integer> index = new HashIndexedArray<>(in);
    for (int i = 0; i < in.length; ++i) {
      Assertions.assertTrue(index.contains(in[i]));
      Assertions.assertEquals(in[i], index.get(i));
      Assertions.assertEquals(i, index.getIndex(in[i]));
    }
    for (int i = 0; i < out.length; ++i) {
      Assertions.assertFalse(index.contains(out[i]));
      Assertions.assertEquals(-1, index.getIndex(out[i]));
    }
  }
}
