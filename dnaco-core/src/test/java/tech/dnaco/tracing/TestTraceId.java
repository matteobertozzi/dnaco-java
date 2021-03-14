package tech.dnaco.tracing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.bytes.BytesUtil;

public class TestTraceId {
  @Test
  public void testEncode() {
    final String TRACE_ID_HEX_STRING = "91ba9c82d1526573b0255816c3c2d22f";
    final String TRACE_ID_STRING = "qnKGkk6GmkD-vsQt6u87uJt";
    final byte[] TRACE_ID_BYTES = new byte[] {
      (byte)145, (byte)186, (byte)156, (byte)130, (byte)209, (byte)82, (byte)101, (byte)115,
      (byte)176, (byte)37, (byte)88, (byte)22, (byte)195, (byte)194, (byte)210, (byte)47
    };

    final TraceId traceId = TraceId.fromString(TRACE_ID_STRING);
    Assertions.assertEquals(TRACE_ID_STRING, traceId.toString());
    Assertions.assertEquals(-7945866506870299277L, traceId.getHi());
    Assertions.assertEquals(-5754096094099090897L, traceId.getLo());
    Assertions.assertEquals(traceId, TraceId.fromBytes(TRACE_ID_BYTES));
    Assertions.assertEquals(TRACE_ID_HEX_STRING, BytesUtil.toHexString(TRACE_ID_BYTES));
    Assertions.assertEquals(traceId, TraceId.fromString(TRACE_ID_HEX_STRING));

    for (int i = 0; i < 16; ++i) {
      final TraceId randId = TraceId.newRandomId();
      Assertions.assertEquals(randId, TraceId.fromString(randId.toString()));
    }
  }
}
