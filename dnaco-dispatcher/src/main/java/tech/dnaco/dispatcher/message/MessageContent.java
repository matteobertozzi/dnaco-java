package tech.dnaco.dispatcher.message;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.bytes.ByteArrayWriter;
import tech.dnaco.data.DataFormat;
import tech.dnaco.hashes.Hash;

public interface MessageContent {
  MessageContent retain();
  MessageContent release();

  /**
   * @return the length of the message content
   */
  int contentLength();

  long writeContentToStream(OutputStream stream) throws IOException;
  long writeContentToStream(DataOutput stream) throws IOException;
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

  default Hash contentHash(final Hash hash) {
    return hash.update(convertContentToBytes());
  }
}
