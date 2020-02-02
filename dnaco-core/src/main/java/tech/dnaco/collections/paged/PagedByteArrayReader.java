package tech.dnaco.collections.paged;

import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.io.BytesInputStream;

public class PagedByteArrayReader extends BytesInputStream {
  private final byte[][] pages;
  private final int pageCount;
  private final int length;

  private int readIndex;

  public PagedByteArrayReader(final byte[][] pages, final int pageCount, final int length) {
    this.pages = pages;
    this.pageCount = pageCount;
    this.length = length;
    this.readIndex = 0;
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public boolean isEmpty() {
    return length == 0;
  }

  @Override
  public boolean isNotEmpty() {
    return length != 0;
  }

  @Override
  public void reset() {
    this.readIndex = 0;
  }

  @Override
  public void seekTo(final int offset) {
    this.readIndex = offset;
  }

  @Override
  public int read() throws IOException {
    final int pageSize = pages[0].length;
    final int pageIndex = readIndex / pageSize;
    final int pageOffset = readIndex & (pageSize - 1);
    readIndex++;
    return pages[pageIndex][pageOffset] & 0xff;
  }

  @Override
  public void copyTo(final int blockLen, final OutputStream stream) throws IOException {
    for (int i = 0; i < blockLen; ++i) {
      stream.write(read());
    }
  }
}
