/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.collections.arrays.paged;

import java.io.IOException;
import java.util.Arrays;

import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.arrays.ByteArray;
import tech.dnaco.util.BitUtil;

public class PagedByteArray {
  private static final int DEFAULT_PAGES_GROWTH = 16;
  private static final int DEFAULT_PAGE_SIZE = 1024;

  private final int pageSize;

  private int pageItems;
  private int pageCount;
  private byte[] lastPage;
  private byte[][] pages;

  public PagedByteArray() {
    this(DEFAULT_PAGE_SIZE);
  }

  public PagedByteArray(final int pageSize) {
    this.pageSize = BitUtil.nextPow2(pageSize);

    this.pageItems = 0;
    this.pageCount = 1;
    this.lastPage = new byte[this.pageSize];
    this.pages = null;
  }

  public int size() {
    // NOTE: pageCount expected to always be > 0
    return ((pageCount - 1) * pageSize) + pageItems;
  }

  public boolean isEmpty() {
    // NOTE: pageCount expected to always be > 0
    return pageItems == 0 && pageCount == 1;
  }

  public boolean isNotEmpty() {
    return pageItems > 0 || pageCount > 1;
  }

  // ================================================================================
  //  PUBLIC toByteArray related methods
  // ================================================================================
  public byte[] toByteArray() {
    return toByteArray(0);
  }

  public byte[] toByteArray(final int off) {
    return toByteArray(off, size() - off);
  }

  public byte[] toByteArray(final int off, final int len) {
    try {
      final ByteArray array = new ByteArray(len);
      forEach(off, len, array::add);
      return array.drain();
    } catch (final Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public void add(final PagedByteArray buffer) {
    try {
      buffer.forEach(this::add);
    } catch (final Throwable e) {
      throw new RuntimeException(e);
    }
  }

  // ================================================================================
  //  PUBLIC clear related methods
  // ================================================================================
  public void clear() {
    clear(false);
  }

  public void clear(final boolean forceEviction) {
    this.lastPage = pages != null ? pages[0] : lastPage;
    this.pageCount = 1;
    this.pageItems = 0;
    if (forceEviction && pages != null) {
      for (int i = 1, n = pages.length; i < n; ++i) {
        this.pages[i] = null;
      }
      this.pages = null;
    }
  }

  // ================================================================================
  //  PUBLIC write related methods
  // ================================================================================
  public void add(final int value) {
    if (pageItems == pageSize) rollPage();
    lastPage[pageItems++] = (byte) (value & 0xff);
  }

  public void add(final byte[] buf) {
    add(buf, 0, buf.length);
  }

  public void add(final byte[] buf, int off, int len) {
    while (len > 0) {
      int avail = lastPage.length - pageItems;
      if (avail == 0) {
        rollPage();
        avail = pageSize;
      }

      final int copyLen = Math.min(avail, len);
      System.arraycopy(buf, off, lastPage, pageItems, copyLen);
      pageItems += copyLen;
      off += copyLen;
      len -= copyLen;
    }
  }

  public void set(final int index, final int value) {
    final int pageIndex = index / pageSize;
    final int pageOffset = index & (pageSize - 1);
    if (pages == null && pageIndex == 0) {
      lastPage[pageOffset] = (byte) (value & 0xff);
    } else {
      pages[pageIndex][pageOffset] = (byte) (value & 0xff);
    }
  }

  public void set(final int index, final byte[] buf, final int off, final int len) {
    // TODO: speedup
    for (int i = 0; i < len; ++i) {
      set(index + i, buf[off + i] & 0xff);
    }
  }

  public void fill(final int value) {
    final byte bValue = (byte) (value & 0xff);
    if (pages == null) {
      Arrays.fill(lastPage, bValue);
    } else {
      for (int i = 0, n = pages.length; i < n; ++i) {
        Arrays.fill(pages[i], bValue);
      }
    }
  }

  public void addFixed32(final int value) {
    addFixed(4, value);
  }

  public void addFixed64(final long value) {
    addFixed(8, value);
  }

  public void addFixed(final int bytesWidth, final long value) {
    for (int i = 0; i < bytesWidth; ++i) {
      add((int)((value >>> (i << 3)) & 0xff));
    }
  }

  public void addBlob8(final String value) {
    addBlob8(value.getBytes());
  }

  public void addBlob8(final byte[] value) {
    add(value.length & 0xff);
    add(value);
  }

  public void addBlob(final String value) {
    addBlob(value.getBytes());
  }

  public void addBlob(final byte[] value) {
    VarInt.write(this, value.length);
    add(value);
  }

  public void setFixed32(final int offset, final int value) {
    setFixed(offset, 4, value);
  }

  public void setFixed64(final int offset, final int value) {
    setFixed(offset, 8, value);
  }

  public void setFixed(final int offset, final int bytesWidth, final long value) {
    for (int i = 0; i < bytesWidth; ++i) {
      set(offset + i, (int) ((value >>> (i << 3)) & 0xff));
    }
  }

  // ================================================================================
  //  PUBLIC read related methods
  // ================================================================================
  public int get(final int index) {
    // TODO: do we need boundary checks
    final int pageIndex = index / pageSize;
    final int pageOffset = index & (pageSize - 1);
    if (pages == null && pageIndex == 0) {
      return lastPage[pageOffset] & 0xff;
    }
    return pages[pageIndex][pageOffset] & 0xff;
  }

  public void get(final int index, final byte[] buf, final int off, final int len) {
    for (int i = 0; i < len; ++i) {
      buf[off + i] = (byte) get(index + i);
    }
  }

  public int getFixed32(final int offset) {
    return (int) getFixed(4, offset);
  }

  public long getFixed64(final int offset) {
    return getFixed(8, offset);
  }

  public long getFixed(final int bytesWidth, final int offset) {
    long result = 0;
    for (int i = 0; i < bytesWidth; ++i) {
      result += ((long)get(offset + i) & 0xff) << (i << 3);
    }
    return result;
  }

  public byte[] getBlob8(final int offset) {
    final int buflen = get(offset) & 0xff;
    final byte[] buffer = new byte[buflen];
    for (int i = 0; i < buflen; ++i) {
      buffer[i] = (byte) get(offset + i + 1);
    }
    return buffer;
  }

  // ================================================================================
  //  PRIVATE foreach
  // ================================================================================
  public interface ByteArrayConsumer {
    void accept(byte[] buf, int off, int len) throws IOException;
  }

  public int forEach(final ByteArrayConsumer consumer) throws IOException {
    if (pages == null) {
      consumer.accept(lastPage, 0, pageItems);
      return pageItems;
    }

    return forEach(0, size(), consumer);
  }

  public int forEach(final int off, int len, final ByteArrayConsumer consumer) throws IOException {
    if (len == 0) return 0;

    final int pageIndex = off / pageSize;
    final int pageOffset = off & (pageSize - 1);
    if (pageCount == 1) {
      final int avail = Math.min(len, pageItems - pageOffset);
      consumer.accept(lastPage, pageOffset, avail);
      return avail;
    }

    int wlen = 0;
    int avail = Math.min(len, pageSize - pageOffset);
    consumer.accept(pages[pageIndex], pageOffset, avail);
    wlen += avail;
    len -= avail;
    for (int i = pageIndex + 1, n = pageCount - 1; len > 0 && i < n; ++i) {
      avail = Math.min(len, pageSize);
      consumer.accept(pages[i], 0, avail);
      wlen += avail;
      len -= avail;
    }
    if (len > 0) {
      avail = Math.min(len, pageItems);
      consumer.accept(lastPage, 0, avail);
      wlen += avail;
    }
    return wlen;
  }

  // ================================================================================
  //  PRIVATE helpers
  // ================================================================================
  private void rollPage() {
    if (pages == null) {
      pages = new byte[DEFAULT_PAGES_GROWTH][];
      pages[0] = lastPage;
    } else if (pageCount == pages.length) {
      pages = Arrays.copyOf(pages, pages.length + DEFAULT_PAGES_GROWTH);
    }
    lastPage = pages[pageCount];
    if (lastPage == null) {
      lastPage = new byte[pageSize];
    }
    pages[pageCount++] = lastPage;
    pageItems = 0;
  }
}
