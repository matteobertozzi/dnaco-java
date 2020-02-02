package tech.dnaco.collections.paged;

import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.io.BytesOutputStream;

public class PagedByteArrayWriter extends BytesOutputStream {
  private final PagedByteArray buf;

  public PagedByteArrayWriter(final int blockSize) {
    this(new PagedByteArray(blockSize));
  }

  public PagedByteArrayWriter(final PagedByteArray buffer) {
    this.buf = buffer;
  }

  public byte[] toByteArray() {
    final byte[] data = new byte[buf.size()];
    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte) (buf.get(i) & 0xff);
    }
    return data;
  }

  @Override
  public void reset() {
    buf.clear();
  }

  @Override
  public boolean isEmpty() {
    return buf.isEmpty();
  }

  @Override
  public int length() {
    return buf.size();
  }

  @Override
  public void write(int b) {
    buf.add(b);
  }

  @Override
  public void write(byte[] buf, int off, int len) {
    this.buf.add(buf, off, len);
  }

  @Override
  public int writeTo(BytesOutputStream stream) {
    return this.buf.forEach((buf, off, len) -> stream.write(buf, off, len));
  }

  @Override
  public int writeTo(OutputStream stream) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void writeTo(BytesOutputStream stream, int off, int len) {
    // TODO Auto-generated method stub

  }

  @Override
  public void writeTo(OutputStream stream, int off, int len) throws IOException {
    // TODO Auto-generated method stub

  }
}
