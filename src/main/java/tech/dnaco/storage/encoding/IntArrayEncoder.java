package tech.dnaco.storage.encoding;

import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.strings.HumansUtil;

public class IntArrayEncoder {
  private IntArrayEncoder() {
    // no-op
  }

  public static void encodeSequence(final OutputStream writer, final int[] buf, final int off, final int len)
      throws IOException {
    if (len == 0) {
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, 0);
      return;
    }

    if (len == 1) {
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, 1);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, buf[off]);
      return;
    }

    int minValue = buf[off];
    int maxValue = minValue;
    for (int i = 1; i < len; ++i) {
      final int v = buf[off + i] - buf[off + i - 1];
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
    }

    final int width = IntUtil.getWidth(maxValue - minValue);

    if (false) {
      final int maxWidth = IntUtil.getWidth(maxValue);
      System.out.println(" --> INDEX MIN-VALUE " + minValue + " MAX-VALUE " + maxValue +
                         " WIDTH " + maxWidth + " -> " + width +
                         " -> " + width + " total " + HumansUtil.humanSize(((len * width) + 7) / 8));
    }
    IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, len);
    IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, minValue);
    IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, width);
    //System.out.println("ENCODE");
    //System.out.println(" - len=" + len);
    //System.out.println(" - minValue=" + minValue);
    //System.out.println(" - width=" + width);
    try (BitEncoder encoder = new BitEncoder(writer, width)) {
      encoder.add(buf[off] - minValue);
      for (int i = 1; i < len; ++i) {
        encoder.add((buf[off + i] - buf[off + i - 1]) - minValue);
      }
    }
  }
}
