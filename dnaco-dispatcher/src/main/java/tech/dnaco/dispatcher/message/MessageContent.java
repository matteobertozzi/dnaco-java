package tech.dnaco.dispatcher.message;

import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.bytes.ByteArrayWriter;
import tech.dnaco.data.DataFormat;

public interface MessageContent {
  /**
   * @return the length of the message content
   */
  int contentLength();

  long writeContentToStream(OutputStream stream) throws IOException;
  <T> T convertContent(DataFormat format, Class<T> classOfT);

  default boolean hasContent() {
    return contentLength() > 0;
  }

  default byte[] convertContentToBytes() {
    final byte[] buffer = new byte[contentLength()];
    try (ByteArrayWriter writer = new ByteArrayWriter(buffer)) {
      writeContentToStream(writer);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return buffer;
  }
}
