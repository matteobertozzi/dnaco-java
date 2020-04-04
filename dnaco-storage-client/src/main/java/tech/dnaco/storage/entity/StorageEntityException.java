package tech.dnaco.storage.entity;

import tech.dnaco.storage.StorageException;

public class StorageEntityException extends StorageException {
  private static final long serialVersionUID = -2024042293981711820L;

  public StorageEntityException(final Throwable cause) {
    super(cause);
  }
}