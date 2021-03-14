package tech.dnaco.collections.arrays.paged;

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
  public void write(final int b) {
    buf.add(b);
  }

  @Override
  public void write(final byte[] buf, final int off, final int len) {
    this.buf.add(buf, off, len);
  }

  @Override
  public int writeTo(final BytesOutputStream stream) {
    try {
      return this.buf.forEach(stream::write);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int writeTo(final OutputStream stream) throws IOException {
    return this.buf.forEach(stream::write);
  }

  @Override
  public void writeTo(final BytesOutputStream stream, final int off, final int len) {
    try {
      this.buf.forEach(off, len, stream::write);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeTo(final OutputStream stream, final int off, final int len) throws IOException {
    this.buf.forEach(off, len, stream::write);
  }
}
