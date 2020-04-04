package tech.dnaco.storage;

import java.io.IOException;

public class StorageException extends IOException {
	private static final long serialVersionUID = -5292946248584429722L;

  public StorageException(final String message) {
    super(message);
  }

  public StorageException(final Throwable cause) {
    super(cause);
  }

  public StorageException(final Throwable cause, final String message) {
    super(message, cause);
  }
}