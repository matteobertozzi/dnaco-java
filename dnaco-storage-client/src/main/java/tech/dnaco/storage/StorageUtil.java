package tech.dnaco.storage;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public final class StorageUtil {
  private StorageUtil() {
    // no-op
  }

  public static String path(final String... component) {
    final StringBuilder builder = new StringBuilder(component.length * 5);
    builder.append('/');
    for (int i = 0; i < component.length; ++i) {
      final String part = component[i];
      final int startOffset = part.charAt(0) == '/' ? 1 : 0;
      final int endOffset = part.charAt(part.length() - 1) == '/' ? 1 : 0;
      builder.append(part, startOffset, part.length() - endOffset);
      builder.append('/');
    }
    return builder.toString();
  }

  public static ByteBuf toByteBuf(final byte[] value) {
    return Unpooled.wrappedBuffer(value);
  }

  public static ByteBuf toByteBuf(final String value) {
    return Unpooled.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8));
  }

  public static void main(String[] args) {
    System.out.println(path());
    System.out.println(path("foo"));
    System.out.println(path("foo", "bar"));
    System.out.println(path("/foo", "/bar"));
    System.out.println(path("/foo/", "/bar"));
  }
}