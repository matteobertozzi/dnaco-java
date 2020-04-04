package tech.dnaco.storage;

public class StorageKeyNotFoundException extends StorageException {
  private static final long serialVersionUID = 3668068077470590682L;

  public StorageKeyNotFoundException(final String key) {
    super(String.format("key '%s' not found", key));
  }

  public StorageKeyNotFoundException(final Throwable cause) {
    super(cause);
  }
}
