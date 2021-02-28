package tech.dnaco.tracing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTraceId {
  @Test
  public void testEncode() {
    final TraceId traceId = TraceId.fromString("RNkhLL6hMLe-WTqU6V87VjU");
    Assertions.assertEquals("RNkhLL6hMLe-WTqU6V87VjU", traceId.toString());
    Assertions.assertEquals(-7945866506870299277L, traceId.getHi());
    Assertions.assertEquals(-5754096094099090897L, traceId.getLo());
    Assertions.assertEquals(traceId, TraceId.fromBytes(new byte[] {
      (byte)145, (byte)186, (byte)156, (byte)130, (byte)209, (byte)82, (byte)101, (byte)115,
      (byte)176, (byte)37, (byte)88, (byte)22, (byte)195, (byte)194, (byte)210, (byte)47
    }));

    for (int i = 0; i < 16; ++i) {
      final TraceId randId = TraceId.newRandomId();
      Assertions.assertEquals(randId, TraceId.fromString(randId.toString()));
    }
  }
}
