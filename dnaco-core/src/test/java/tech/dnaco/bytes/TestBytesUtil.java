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
}
