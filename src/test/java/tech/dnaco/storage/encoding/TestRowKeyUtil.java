package tech.dnaco.storage.encoding;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.storage.demo.RowKeyUtil;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;

public class TestRowKeyUtil {
  private static final byte[][] ROW_KEY = new byte[][] {
    new byte[] { 1 },
    new byte[] { 1, 0, 2 },
    new byte[] { 1, 0, 1, 0, 2 },
    new byte[] { 1, 2, 0, 1, 3, 0, 4, 5, 0, 0, 1 },
    new byte[] { 1, 2, 0, 1, 3, 0, 4, 5, 0, 6 },
    new byte[] { 1, 2, 0, 1, 0, 1, 3, 0, 4, 5, 0, 1, 0, 6, 0, 1 }
  };

  private static final byte[][][] KEYS = new byte[][][] {
    new byte[][] { new byte[] { 1 } },
    new byte[][] { new byte[] { 1 }, new byte[] { 2 } },
    new byte[][] { new byte[] { 1, 0 }, new byte[] { 2 } },
    new byte[][] { new byte[] { 1, 2, 0, 3 }, new byte[] { 4, 5 }, new byte[] { 0 } },
    new byte[][] { new byte[] { 1, 2, 0, 3 }, new byte[] { 4, 5 }, new byte[] { 6 } },
    new byte[][] { new byte[] { 1, 2, 0, 0, 3 }, new byte[] { 4, 5, 0 }, new byte[] { 6, 0 } }
  };

  @Test
  public void testEncode() {
    for (int i = 0; i < KEYS.length; ++i) {
      final RowKeyBuilder rowKey = new RowKeyBuilder();
      for (int k = 0; k < KEYS[i].length; ++k) {
        rowKey.add(KEYS[i][k]);
      }
      Assertions.assertArrayEquals(rowKey.drain(), ROW_KEY[i]);
    }
  }

  @Test
  public void testDecode() {
    for (int i = 0; i < ROW_KEY.length; ++i) {
      final int keyIndex = i;
      final AtomicInteger index = new AtomicInteger();
      RowKeyUtil.decodeKey(ROW_KEY[i], (key) -> {
        Assertions.assertArrayEquals(key, KEYS[keyIndex][index.get()]);
        index.incrementAndGet();
      });
    }
  }

  @Test
  public void testFoo() {
    final byte[] a = new RowKeyBuilder().add("k1").add("k2").add("f1").drain();
    final byte[] b = new RowKeyBuilder().add("k2").add("k2").add("f3").drain();
    final byte[] c = new RowKeyBuilder().add("k1").add("k2").add("f0").add("foo").drain();
    final byte[] d = new RowKeyBuilder().add("k1").add("k2").add("f2").add("foo").drain();
    final byte[][] values = new byte[][] { a, b, c, d };
    Arrays.sort(values, (A, B) -> BytesUtil.compare(A, B));

    for (int i = 0; i < values.length; ++i) {
      System.out.println(BytesUtil.toString(values[i]));
    }
  }
}
