package tech.dnaco.data.encoding;

import java.util.function.LongConsumer;

import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.util.BitUtil;

public final class IntSeqCoding {
  public static final SliceType[] SLICE_TYPES = SliceType.values();

  public enum SliceType { RLE, LIN, MIN, EOF }

  private IntSeqCoding() {
    // no-op
  }

  interface IntSeqEncoder {
    void add(SliceType type, int start, int end, long value, long delta);

    int rleBits(int length, long value);
    int linBits(int length, long value, long delta);
    int minBits(int length, long minValue, long delta);
  }

  public static void decode(final byte[] buffer, final int off, final LongConsumer consumer) {
    final BitDecoder bitDecoder = new BitDecoder(buffer, off);
    while (true) {
      switch (SLICE_TYPES[bitDecoder.readAsInt(2)]) {
        case RLE:
          readRle(bitDecoder, consumer);
          break;
        case LIN:
          readLin(bitDecoder, consumer);
          break;
        case MIN:
          readMin(bitDecoder, consumer);
          break;
        case EOF:
          return;
      }
    }
  }

  private static class Encoder implements IntSeqEncoder {
    private final BitEncoder bitEncoder;
    private final long[] seq;

    private Encoder(final long[] seq) {
      this.bitEncoder = new BitEncoder(1 << 10);
      this.seq = seq;
    }

    public void add(final SliceType type, final int start, final int end, final long value, final long delta) {
      switch (type) {
        case RLE -> writeRle(bitEncoder, end - start, value);
        case LIN -> writeLin(bitEncoder, end - start, value, delta);
        case MIN -> writeMin(bitEncoder, seq, start, end, value, delta);
        case EOF -> bitEncoder.add(SliceType.EOF.ordinal(), 2);
      }
    }

    @Override
    public int rleBits(final int length, final long value) {
      return IntSeqCoding.rleBits(length, value);
    }

    @Override
    public int linBits(final int length, final long value, final long delta) {
      return IntSeqCoding.linBits(length, value, delta);
    }

    @Override
    public int minBits(final int length, final long minValue, final long delta) {
      return IntSeqCoding.minBits(length, minValue, delta);
    }
  }

  /*
   * RLE Format
   *   |-------- 2 bits: encoding type (RLE, LIN or MIN)
   *   |    |-------- 2 bits: size in bits of the length of the encoded sequence (0:4bit, 1:8bit, 2:12bit, 3:16bit)
   *   |    |    |-------- 6 bits: size in bits of the "base" value
   *   |    |    |       |-------- N bits: length of the encoded sequence
   *   |    |    |       |        |-------- N bits: value
   *   v    v    v       v        v
   * +----+----+--------+--------+--------+
   * | 00 | .. | ...... | ...... | ...... |
   * +----+----+--------+--------+--------+
   */
  public static int rleBits(final int length, final long value) {
    return 10 + lengthBits(length) + valueBits(value);
  }

  public static void writeRle(final BitEncoder bitEncoder, final int length, final long value) {
    final int lengthBits = lengthBits(length);
    final int valueBits = valueBits(value);
    bitEncoder.add(SliceType.RLE.ordinal(), 2);
    bitEncoder.add((lengthBits >> 2) - 1, 2);
    bitEncoder.add(valueBits - 1, 6);
    bitEncoder.add(length, lengthBits);
    bitEncoder.add(value, valueBits);
  }

  public static void readRle(final BitDecoder bitDecoder, final LongConsumer consumer) {
    final int lengthBits = (1 + bitDecoder.readAsInt(2)) << 2;
    final int valueBits = 1 + bitDecoder.readAsInt(6);
    final int length = bitDecoder.readAsInt(lengthBits);
    final long value = bitDecoder.read(valueBits);
    for (int i = 0; i < length; ++i) {
      consumer.accept(value);
    }
  }

  /*
   * LIN Format
   *   |-------- 2 bits: encoding type (RLE, LIN or MIN)
   *   |    |-------- 2 bits: size in bits of the length of the encoded sequence (0:4bit, 1:8bit, 2:12bit, 3:16bit)
   *   |    |    |-------- 6 bits: size in bits of the "base" value
   *   |    |    |       |-------- 6bits: size in bits of the "delta" values
   *   |    |    |       |          |-------- N bits: length of the encoded sequence
   *   |    |    |       |          |          |-------- `base` bits: base value
   *   |    |    |       |          |          |           |-------- `delta` bits: delta
   *   v    v    v       v          v          v           v
   * +----+----+--------+--------+ +--------+ +------------+-------------+
   * | 01 | xx | xxxxxx | xxxxxx | | length | | base value | delta value |
   * +----+----+--------+--------+ +--------+ +------------+-------------+
   */
  public static int linBits(final int length, final long value, final long delta) {
    return 16 + lengthBits(length) + valueBits(value) + signedValueBits(delta);
  }

  public static void writeLin(final BitEncoder bitEncoder, final int length, final long baseValue, final long delta) {
    final int lengthBits = lengthBits(length);
    final int baseBits = valueBits(baseValue);
    final int deltaBits = signedValueBits(delta);
    bitEncoder.add(SliceType.LIN.ordinal(), 2);
    bitEncoder.add((lengthBits >> 2) - 1, 2);
    bitEncoder.add(baseBits - 1, 6);
    bitEncoder.add(deltaBits - 1, 6);
    bitEncoder.add(length, lengthBits);
    bitEncoder.add(baseValue, baseBits);
    bitEncoder.addSigned(delta, deltaBits);
  }

  public static void readLin(final BitDecoder bitDecoder, final LongConsumer consumer) {
    final int lengthBits = (1 + bitDecoder.readAsInt(2)) << 2;
    final int baseBits = 1 + bitDecoder.readAsInt(6);
    final int deltaBits = 1 + bitDecoder.readAsInt(6);
    final int length = bitDecoder.readAsInt(lengthBits);
    final long baseValue = bitDecoder.read(baseBits);
    final long delta = bitDecoder.readSigned(deltaBits);
    for (int i = 0; i < length; ++i) {
      consumer.accept(baseValue + (i * delta));
    }
  }

  /*
   * MIN Format
   *   |-------- 2 bits: encoding type (RLE, LIN or MIN)
   *   |    |-------- 2 bits: size in bits of the length of the encoded sequence (0:4bit, 1:8bit, 2:12bit, 3:16bit)
   *   |    |    |-------- 6 bits: size in bits of the "min" value
   *   |    |    |       |-------- 6bits: size in bits of the "delta" values
   *   |    |    |       |          |-------- N bits: length of the encoded sequence
   *   |    |    |       |          |          |-------- `min` bits: min value
   *   |    |    |       |          |          |           |-------- `delta` bits: deltas
   *   v    v    v       v          v          v           v
   * +----+----+--------+--------+ +--------+ +-----------+----------+-----
   * | 10 | xx | xxxxxx | xxxxxx | | length | | min value | delta[0] | ...
   * +----+----+--------+--------+ +--------+ +-----------+----------+-----
   */
  public static int minBits(final int length, final long minValue, final long delta) {
    return 16 + lengthBits(length) + valueBits(minValue) + (length * signedValueBits(delta));
  }

  public static void writeMin(final BitEncoder bitEncoder, final long[] seq, final int start, final int end,
      final long minValue, final long delta) {
    final int length = end - start;
    final int lengthBits = lengthBits(length);
    final int minValueBits = valueBits(minValue);
    final int deltaBits = signedValueBits(delta);
    bitEncoder.add(SliceType.MIN.ordinal(), 2);
    bitEncoder.add((lengthBits >> 2) - 1, 2);
    bitEncoder.add(minValueBits - 1, 6);
    bitEncoder.add(deltaBits - 1, 6);
    bitEncoder.add(length, lengthBits);
    bitEncoder.add(minValue, minValueBits);
    for (int i = start; i < end; ++i) {
      bitEncoder.addSigned(seq[i] - minValue, deltaBits);
    }
  }

  public static void readMin(final BitDecoder bitDecoder, final LongConsumer consumer) {
    final int lengthBits = (1 + bitDecoder.readAsInt(2)) << 2;
    final int minValueBits = 1 + bitDecoder.readAsInt(6);
    final int deltaBits = 1 + bitDecoder.readAsInt(6);
    final int length = bitDecoder.readAsInt(lengthBits);
    final long minValue = bitDecoder.read(minValueBits);
    for (int i = 0; i < length; ++i) {
      final long delta = bitDecoder.readSigned(deltaBits);
      final long value = minValue + delta;
      consumer.accept(value);
    }
  }

  // ===========================================================================
  //  Helpers
  // ===========================================================================
  private static int lengthBits(final int length) {
    if (length == 0) {
      throw new IllegalArgumentException("expected length >= 0");
    }
    final int bits = IntUtil.getWidth(length);
    return BitUtil.align(bits, 4);
  }

  private static int valueBits(final long value) {
    if (value < 0) {
      throw new IllegalArgumentException("expected value >= 0, got " + value);
    }
    return value == 0 ? 1 : (64 - Long.numberOfLeadingZeros(value));
  }

  private static int signedValueBits(final long value) {
    return valueBits(Math.abs(value)) + 1;
  }
}
