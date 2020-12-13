package tech.dnaco.storage.encoding;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;

public class DeltaBytesEncoder {
  private final byte[] lastValue;
  private int length;

  public DeltaBytesEncoder(final int maxValueLength) {
    this.lastValue = new byte[maxValueLength];
    this.length = 0;
  }

  public void reset() {
    this.length = 0;
  }

  public int add(final ByteArraySlice value) {
    return add(value.rawBuffer(), value.offset(), value.length());
  }

  public int add(final byte[] buf, final int off, final int len) {
    final int prefix = BytesUtil.prefix(lastValue, 0, length, buf, off, len);
    System.arraycopy(buf, off, lastValue, 0, len);
    this.length = len;
    return prefix;
  }

  public ByteArraySlice getValue() {
    return new ByteArraySlice(lastValue, 0, length);
  }
}
