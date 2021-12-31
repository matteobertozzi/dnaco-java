package tech.dnaco.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LimitedInputStream extends FilterInputStream {
  private final long maxReadable;

  private long consumed;

  public LimitedInputStream(final InputStream in, final long maxReadable) {
    super(in);
    this.maxReadable = maxReadable;
    this.consumed = 0;
  }

  public long consumed() {
    return consumed;
  }

  public int read() throws IOException {
    if (consumed == maxReadable) return -1;

    final int c = super.read();
    if (c >= 0) consumed++;
    return c;
  }

  public int read(final byte[] b) throws IOException {
    return this.read(b, 0, b.length);
  }

  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (consumed == maxReadable) return -1;

    final long avail = maxReadable - consumed;
    final int n = super.read(b, off, (len > avail) ? (int) avail : len);
    if (n >= 0) consumed += n;
    return n;
  }
}
