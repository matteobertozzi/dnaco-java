package tech.dnaco.dispatcher.message;

public class MessageException extends Exception {
  private final MessageError error;

  public MessageException(final MessageError error) {
    super(error.message());
    this.error = error;
  }

  public MessageException(final Throwable e, final MessageError error) {
    super(error.message(), e);
    this.error = error;
  }

  public MessageError getMessageError() {
    return error;
  }
}
