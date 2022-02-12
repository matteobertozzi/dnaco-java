package tech.dnaco.bytes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBytesUtil {
  @Test
  public void testHasPrefix() {
    Assertions.assertTrue(BytesUtil.hasPrefix(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 }));
    Assertions.assertTrue(BytesUtil.hasPrefix(new byte[] { 1, 2, 3, 4 }, new byte[] { 1, 2, 3 }));
    Assertions.assertFalse(BytesUtil.hasPrefix(new byte[] { 1, 2, 4 }, new byte[] { 1, 2, 3 }));
    Assertions.assertFalse(BytesUtil.hasPrefix(new byte[] { 6, 5, 4 }, new byte[] { 1, 2, 3 }));
  }

  @Test
  public void testPrefix() {
    final byte[] full = "hello world".getBytes();
    final byte[] prefixA = "hello worldo".getBytes();
    final byte[] prefixB = "hello".getBytes();
    final byte[] prefixC = "hello boom".getBytes();
    final byte[] prefixD = "booom".getBytes();

    Assertions.assertEquals(11, BytesUtil.prefix(full, full));
    Assertions.assertEquals(11, BytesUtil.prefix(full, prefixA));
    Assertions.assertEquals(5, BytesUtil.prefix(full, prefixB));
    Assertions.assertEquals(6, BytesUtil.prefix(full, prefixC));
    Assertions.assertEquals(0, BytesUtil.prefix(full, prefixD));
  }
}
