package tech.dnaco.dispatcher.message;

import java.io.IOException;

public class MessageException extends IOException {
  private final MessageError error;
  private final boolean logTrace;

  public MessageException(final MessageError error) {
    this(error, true);
  }

  public MessageException(final MessageError error, final boolean logTrace) {
    super(error.message());
    this.error = error;
    this.logTrace = logTrace;
  }

  public MessageException(final Throwable e, final MessageError error) {
    this(e, error, true);
  }

  public MessageException(final Throwable e, final MessageError error, final boolean logTrace) {
    super(error.message(), e);
    this.error = error;
    this.logTrace = logTrace;
  }

  public boolean shouldLogTrace() {
    return logTrace;
  }

  public MessageError getMessageError() {
    return error;
  }
}
