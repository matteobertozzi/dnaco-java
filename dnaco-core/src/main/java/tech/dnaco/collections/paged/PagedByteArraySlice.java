package tech.dnaco.collections.paged;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesSlice;
import tech.dnaco.collections.ArrayUtil.ByteArrayConsumer;

public class PagedByteArraySlice implements BytesSlice {
  private final byte[][] pages;
  private final int pageCount;
  private final int length;

  public PagedByteArraySlice(final byte[][] pages, final int pageCount, final int length) {
    this.pages = pages;
    this.pageCount = pageCount;
    this.length = length;
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
  public int get(final int index) {
    final int pageSize = pages[0].length;
    final int pageIndex = index / pageSize;
    final int pageOffset = index & (pageSize - 1);
    return pages[pageIndex][pageOffset] & 0xff;
  }

  @Override
  public void copyTo(final byte[] buf, final int off, final int len) {
    for (int i = 0; i < len; ++i) {
      buf[off + i] = (byte) (get(i) & 0xff);
    }
  }

  @Override
  public void forEach(final ByteArrayConsumer consumer) {
    forEach(pages, length, consumer);
  }

  @Override
  public void forEach(final int off, final int len, final ByteArrayConsumer consumer) {
    // TODO Auto-generated method stub

  }

  public static void forEach(final byte[][] pages, final int length, final ByteArrayConsumer consumer) {
    for (int i = 0, avail = length; avail > 0; ++i) {
      final int size = Math.min(pages[i].length, avail);
      consumer.accept(pages[i], 0, size);
      avail -= size;
    }
  }

  @Override
  public int compareTo(final BytesSlice o) {
    return ByteArraySlice.slowCompare(this, o);
  }
}
