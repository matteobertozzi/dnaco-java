package tech.dnaco.dispatcher.message;

import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.data.DataFormat;
import tech.dnaco.dispatcher.message.MessageUtil.EmptyMetadata;
import tech.dnaco.io.IOUtil;

public class MessageFile implements Message {
  private final MessageMetadata metadata;
  private final File file;

  public MessageFile(final File file) {
    this(EmptyMetadata.INSTANCE, file);
  }

  public MessageFile(final MessageMetadata metadata, final File file) {
    this.metadata = metadata;
    this.file = file;
  }

  public MessageMetadata metadata() {
    return metadata;
  }

  public File file() {
    return file;
  }

  @Override
  public int contentLength() {
    return Math.toIntExact(file.length());
  }

  @Override
  public long writeContentToStream(final OutputStream stream) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(file)) {
      return inputStream.transferTo(stream);
    }
  }

  @Override
  public long writeContentToStream(final DataOutput stream) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(file)) {
      return IOUtil.copy(inputStream, stream);
    }
  }

  @Override
  public <T> T convertContent(final DataFormat format, final Class<T> classOfT) {
    try {
      return format.fromFile(file, classOfT);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int estimateSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Message retain() {
    return this;
  }

  @Override
  public Message release() {
    return this;
  }
}
