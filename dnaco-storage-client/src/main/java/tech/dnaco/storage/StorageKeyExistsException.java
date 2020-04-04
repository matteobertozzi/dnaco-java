package tech.dnaco.storage;

public class StorageKeyExistsException extends StorageException {
  private static final long serialVersionUID = 9062579706096423535L;

  public StorageKeyExistsException(final String key) {
    super(String.format("key '%s' already exists", key));
  }

  public StorageKeyExistsException(final Throwable cause, final String key) {
    super(cause, String.format("key '%s' already exists", key));
  }
}
