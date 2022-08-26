package tech.dnaco.dispatcher.message;

import java.util.List;

import tech.dnaco.dispatcher.message.MessageHandler.UriMethod;

public interface UriMessage extends Message {
  UriMethod method();
  String path();

  MessageMetadata queryParams();

  default List<String> queryParamAsList(final String key) {
    return queryParams().getList(key);
  }

  default String queryParam(final String key, final String defaultValue) {
    return queryParams().getString(key, defaultValue);
  }
}
