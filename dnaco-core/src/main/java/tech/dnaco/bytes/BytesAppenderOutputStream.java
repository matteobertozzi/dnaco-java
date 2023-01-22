package tech.dnaco.bytes;

import java.io.OutputStream;

public class BytesAppenderOutputStream extends OutputStream {
  private final ByteArrayAppender appender;

  public BytesAppenderOutputStream(final ByteArrayAppender appender) {
    this.appender = appender;
  }

  @Override
  public void write(final int b) {
    appender.add(b);
  }

  @Override
  public void write(final byte b[], final int off, final int len) {
    appender.add(b, off, len);
  }
}
