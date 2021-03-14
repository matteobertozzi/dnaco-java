package tech.dnaco.tracing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.bytes.BytesUtil;

public class TestSpanId {
  @Test
  public void testEncode() {
    final String SPAN_ID_HEX_STRING = "bdd55786deecb7a1";
    final String SPAN_ID_STRING = "xKC3yVBDUtt";
    final byte[] SPAN_ID_BYTES = new byte[] {
      (byte)189, (byte)213, (byte)87, (byte)134, (byte)222, (byte)236, (byte)183, (byte)161
    };

    final SpanId spanId = SpanId.fromString(SPAN_ID_STRING);
    Assertions.assertEquals(SPAN_ID_STRING, spanId.toString());
    Assertions.assertEquals(-4767808393724512351L, spanId.getSpanId());
    Assertions.assertEquals(spanId, SpanId.fromBytes(SPAN_ID_BYTES));
    Assertions.assertEquals(SPAN_ID_HEX_STRING, BytesUtil.toHexString(SPAN_ID_BYTES));
    Assertions.assertEquals(spanId, SpanId.fromString(SPAN_ID_HEX_STRING));

    for (int i = 0; i < 16; ++i) {
      final SpanId randId = SpanId.newRandomId();
      Assertions.assertEquals(randId, SpanId.fromString(randId.toString()));
    }
  }
}
