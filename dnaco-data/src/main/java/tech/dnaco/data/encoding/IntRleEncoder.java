package tech.dnaco.data.encoding;

public class IntRleEncoder {
  private long lastValue;
  private long length;
  private long xOffset;
  int offset = 0;

  public void add(final long value) {
    if (value == lastValue) {
      length++;
    } else {
      if (length > 1) {
        System.out.println("RLE: offset:" + xOffset + " value:" + lastValue + " length:" + length);
      }
      xOffset = offset;
      lastValue = value;
      length = 0;
    }
    offset++;
  }
}
