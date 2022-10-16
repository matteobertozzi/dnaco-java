package tech.dnaco.net.http;

import tech.dnaco.dispatcher.message.Message;
import tech.dnaco.dispatcher.message.MessageMetadataMap;
import tech.dnaco.dispatcher.message.MessageUtil;

public final class HttpMessageUtil {
  private HttpMessageUtil() {
    // no-op
  }

  public static Message newMovedPermanently(final String redirectUri) {
    return newRedirect(301, redirectUri);
  }

  public static Message newFound(final String redirectUri) {
    return newRedirect(302, redirectUri);
  }

  public static Message newSeeOther(final String redirectUri) {
    return newRedirect(303, redirectUri);
  }

  public static Message newTemporaryRedirect(final String redirectUri) {
    return newRedirect(307, redirectUri);
  }

  public static Message newPermanentRedirect(final String redirectUri) {
    return newRedirect(308, redirectUri);
  }

  public static Message newRedirect(final int status, final String redirectUri) {
    final MessageMetadataMap metadata = new MessageMetadataMap();
    metadata.set(MessageUtil.METADATA_FOR_HTTP_STATUS, status);
    metadata.set("location", redirectUri);
    return MessageUtil.newEmptyMessage(metadata);
  }

  public static Message newNotModified() {
    final MessageMetadataMap metadata = new MessageMetadataMap();
    metadata.set(MessageUtil.METADATA_FOR_HTTP_STATUS, 304);
    return MessageUtil.newEmptyMessage(metadata);
  }
}
