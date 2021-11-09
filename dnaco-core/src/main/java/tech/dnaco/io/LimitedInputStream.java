package tech.dnaco.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LimitedInputStream extends FilterInputStream {
  private final long maxReadable;

  private long read;

  public LimitedInputStream(final InputStream in, final long maxReadable) {
    super(in);
    this.maxReadable = maxReadable;
    this.read = 0;
  }

  public int read() throws IOException {
    if (read == maxReadable) return -1;

    final int c = super.read();
    if (c >= 0) read++;
    return c;
  }

  public int read(final byte b[]) throws IOException {
    return this.read(b, 0, b.length);
  }

  public int read(final byte b[], final int off, final int len) throws IOException {
    if (read == maxReadable) return -1;

    final long avail = maxReadable - read;
    final int n = super.read(b, off, (len > avail) ? (int) avail : len);
    if (n >= 0) read += n;
    return n;
  }
}
