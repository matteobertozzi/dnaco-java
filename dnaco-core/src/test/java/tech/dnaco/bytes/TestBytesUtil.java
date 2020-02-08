package tech.dnaco.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestBytesUtil {

  @Test
  public void testEquals() {
    assertEquals(true, BytesUtil.equals(null, null));
    assertEquals(true, BytesUtil.equals(new byte[0], new byte[0]));
    assertEquals(true, BytesUtil.equals(null, new byte[0]));
    assertEquals(true, BytesUtil.equals(new byte[0], null));
    assertEquals(true, BytesUtil.equals(new byte[] { 10 }, new byte[] { 10 }));
    assertEquals(false, BytesUtil.equals(new byte[] { 10 }, new byte[] { 20 }));
    assertEquals(false, BytesUtil.equals(new byte[] { 10 }, null));
  }

  @Test
  public void testToHex() {
    final byte[] x = new byte[] { (byte) 0xf1, (byte) 0x0a, (byte) 0xcb, (byte) 0x57 };
    assertEquals("f10acb57", BytesUtil.toHexString(x));

    final byte[] data = new byte[256];
    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte) (i & 0xff);
    }

    assertEquals("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff", new String(BytesUtil.toHexBytes(data)));
  }
}
