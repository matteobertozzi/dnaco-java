package tech.dnaco.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LimitedInputStream extends FilterInputStream {
  private final boolean closeInput;
  private final long maxReadable;

  private long consumed;

  public LimitedInputStream(final InputStream in, final long maxReadable) {
    this(in, maxReadable, true);
  }

  public LimitedInputStream(final InputStream in, final long maxReadable, final boolean closeInput) {
    super(in);
    this.maxReadable = maxReadable;
    this.consumed = 0;
    this.closeInput = closeInput;
  }

  @Override
  public void close() throws IOException {
    if (closeInput) {
      super.close();
    }
  }

  public long consumed() {
    return consumed;
  }

  @Override
  public int read() throws IOException {
    if (consumed == maxReadable) return -1;

    final int c = super.read();
    if (c >= 0) consumed++;
    return c;
  }

  @Override
  public int read(final byte[] b) throws IOException {
    return this.read(b, 0, b.length);
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (consumed == maxReadable) return -1;

    final long avail = maxReadable - consumed;
    final int n = super.read(b, off, (len > avail) ? (int) avail : len);
    if (n >= 0) consumed += n;
    return n;
  }
}
