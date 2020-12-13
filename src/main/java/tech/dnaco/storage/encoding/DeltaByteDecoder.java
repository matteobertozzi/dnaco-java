package tech.dnaco.storage.encoding;

import tech.dnaco.bytes.BytesSlice;

public class DeltaByteDecoder {
  private final byte[] lastValue;
  private int length;

  public DeltaByteDecoder(final int maxValueLength) {
    this.lastValue = new byte[maxValueLength];
    this.length = 0;
  }

  public void reset() {
    this.length = 0;
  }

  public int length() {
    return length;
  }

  public byte[] rawBuffer() {
    return lastValue;
  }

  public void add(final BytesSlice value) {
    value.copyTo(lastValue, 0, value.length());
    this.length = value.length();
  }
}
