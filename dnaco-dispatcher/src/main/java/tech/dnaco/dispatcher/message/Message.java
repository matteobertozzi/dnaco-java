package tech.dnaco.dispatcher.message;

import java.util.List;

public interface Message extends MessageContent {
  /**
   * @return The size of the message. metadata + data
   */
  int estimateSize();

  long timestampNs();

  MessageMetadata metadata();

  default List<String> metadataValueAsList(final String key) {
    return metadata().getList(key);
  }

  default String getMetadata(final String key, final String defaultValue) {
    return metadata().getString(key, defaultValue);
  }
}
