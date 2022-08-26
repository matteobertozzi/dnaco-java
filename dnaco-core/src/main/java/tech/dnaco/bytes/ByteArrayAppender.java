package tech.dnaco.bytes;

public interface ByteArrayAppender {
  void add(int value);
  void add(byte[] buf);
  void add(byte[] buf, int off, int len);
}
