package tech.dnaco.dispatcher.message;

import java.io.IOException;

import tech.dnaco.dispatcher.message.MessageUtil.EmptyMetadata;

public class MessageException extends IOException {
  private final MessageMetadata metadata;
  private final MessageError error;
  private final boolean logTrace;

  public MessageException(final MessageError error) {
    this(error, true);
  }

  public MessageException(final MessageError error, final boolean logTrace) {
    this(EmptyMetadata.INSTANCE, error, logTrace);
  }

  public MessageException(final MessageMetadata metadata, final MessageError error) {
    this(metadata, error, true);
  }

  public MessageException(final MessageMetadata metadata, final MessageError error, final boolean logTrace) {
    super(error.message());
    this.metadata = metadata;
    this.error = error;
    this.logTrace = logTrace;
  }

  public MessageException(final Throwable e, final MessageError error) {
    this(e, error, true);
  }

  public MessageException(final Throwable e, final MessageError error, final boolean logTrace) {
    this(e, EmptyMetadata.INSTANCE, error, logTrace);
  }

  public MessageException(final Throwable e, final MessageMetadata metadata, final MessageError error) {
    this(e, metadata, error, true);
  }

  public MessageException(final Throwable e, final MessageMetadata metadata, final MessageError error, final boolean logTrace) {
    super(error.message(), e);
    this.metadata = metadata;
    this.error = error;
    this.logTrace = logTrace;
  }

  public boolean shouldLogTrace() {
    return logTrace;
  }

  public MessageMetadata getMetadata() {
    return metadata;
  }

  public MessageError getMessageError() {
    return error;
  }
}
