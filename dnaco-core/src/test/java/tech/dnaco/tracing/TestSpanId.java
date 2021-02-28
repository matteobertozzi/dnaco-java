package tech.dnaco.tracing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSpanId {
  @Test
  public void testEncode() {
    final SpanId spanId = SpanId.fromString("Ykd3ZvceuUU");
    Assertions.assertEquals("Ykd3ZvceuUU", spanId.toString());
    Assertions.assertEquals(-4767808393724512351L, spanId.getSpanId());
    Assertions.assertEquals(spanId, SpanId.fromBytes(new byte[] {
      (byte)189, (byte)213, (byte)87, (byte)134, (byte)222, (byte)236, (byte)183, (byte)161
    }));

    for (int i = 0; i < 16; ++i) {
      final SpanId randId = SpanId.newRandomId();
      Assertions.assertEquals(randId, SpanId.fromString(randId.toString()));
    }
  }
}
