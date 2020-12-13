package tech.dnaco.storage.encoding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.logging.Logger;

public class TestBitEncoding {
  @Test
  public void testEncodeDecode() throws Exception {
    final int N = 12;
    final byte[] encodedData;
    try (ByteArrayOutputStream writer = new ByteArrayOutputStream()) {
      try (BitEncoder encoder = new BitEncoder(writer, 9)) {
        for (int i = 0; i < N; ++i) {
          encoder.add(N - i);
        }
      }

      writer.flush();
      encodedData = writer.toByteArray();
    }
    Assertions.assertEquals(14, encodedData.length);
    Logger.debug("Encoded Data {}: {}", encodedData.length, BytesUtil.toHexString(encodedData));

    try (ByteArrayInputStream reader = new ByteArrayInputStream(encodedData)) {
      final BitDecoder decoder = new BitDecoder(reader, 9, N);
      for (int i = 0; i < N; ++i) {
        Assertions.assertEquals(N - i, decoder.read());
      }
    }
  }
}
