package tech.dnaco.storage.encoding;

import java.io.IOException;
import java.io.InputStream;

import tech.dnaco.bytes.encoding.IntDecoder;

public class IntArrayDecoder {
  private IntArrayDecoder() {
    // no-op
  }

  public static int[] decodeSequence(final InputStream stream, final int prefix)
      throws IOException {
    final int len = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    if (len == 0) return new int[prefix];

    final int minValue = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    if (len == 1) {
      final int[] data = new int[prefix + 1];
      data[prefix] = minValue;
      return data;
    }

    final int width = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    //System.out.println("DECODE");
    //System.out.println(" - len=" + len);
    //System.out.println(" - minValue=" + minValue);
    //System.out.println(" - width=" + width);
    final int[] data = new int[prefix + len];
    final BitDecoder bitDecoder = new BitDecoder(stream, width, len);
    data[prefix] = bitDecoder.readInt() + minValue;
    for (int i = 1; i < len; ++i) {
      data[prefix + i] = minValue + data[(prefix + i) - 1] + bitDecoder.readInt();
    }
    return data;
  }
}
