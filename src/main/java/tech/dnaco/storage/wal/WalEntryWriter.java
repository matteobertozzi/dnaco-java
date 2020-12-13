package tech.dnaco.storage.wal;

import java.io.FileOutputStream;

import tech.dnaco.collections.paged.PagedByteArray;

public class WalEntryWriter implements AutoCloseable {
  @Override
  public void close() throws Exception {
    // no-op
  }

  public void add(final FileOutputStream stream, final PagedByteArray entryBuffer, final int entryOffset) {
    // no-op
  }

  public void flush(final FileOutputStream stream) {
    // no-op
  }
}
