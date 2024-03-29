package tech.dnaco.dispatcher.message;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.LongValue;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.data.CborFormat;
import tech.dnaco.data.DataFormat;
import tech.dnaco.data.JsonFormat;
import tech.dnaco.data.XmlFormat;
import tech.dnaco.strings.StringUtil;

public final class MessageUtil {
  public static final String METADATA_FOR_HTTP_METHOD = ":method";
  public static final String METADATA_FOR_HTTP_URI = ":uri";
  public static final String METADATA_FOR_HTTP_STATUS = ":status";
  public static final String METADATA_ACCEPT = "accept";
  public static final String METADATA_CONTENT_TYPE = "content-type";
  public static final String METADATA_CONTENT_LENGTH = "content-length";

  public static final String CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded";
  public static final String CONTENT_TYPE_TEXT_XML = "text/xml";
  public static final String CONTENT_TYPE_APP_XML = "application/xml";
  public static final String CONTENT_TYPE_APP_CBOR = "application/cbor";
  public static final String CONTENT_TYPE_APP_JSON = "application/json";

  private MessageUtil() {
    // no-op
  }

  // ====================================================================================================
  //  Metadata util
  // ====================================================================================================
  public static DataFormat parseAcceptFormat(final MessageMetadata metadata) {
    return parseAcceptFormat(metadata, JsonFormat.INSTANCE);
  }

  public static DataFormat parseAcceptFormat(final MessageMetadata metadata, final DataFormat defaultFormat) {
    return parseAcceptFormat(metadata, defaultFormat, MessageUtil::parseTypeToDataFormat);
  }

  public static <T> T parseAcceptFormat(final MessageMetadata metadata, final T defaultFormat,
      final Function<String, T> parseFormat) {
    final String accept = metadata.getString(METADATA_ACCEPT, null);
    return parseAcceptFormat(accept, defaultFormat, parseFormat);
  }

  public static <T> T parseAcceptFormat(final String accept, final T defaultFormat, final Function<String, T> parseFormat) {
    if (StringUtil.isEmpty(accept)) return defaultFormat;

    T format = parseFormat.apply(accept);
    if (format != null) return format;

    int lastIndex = 0;
    while (lastIndex < accept.length()) {
      int eof = accept.indexOf(',', lastIndex);
      if (eof < 0) eof = accept.length();

      String type = accept.substring(lastIndex, eof);
      final int qIndex = type.lastIndexOf(';');
      if (qIndex > 0) type = type.substring(0, qIndex);

      format = parseFormat.apply(type.trim());
      if (format != null) return format;

      lastIndex = eof + 1;
    }
    return defaultFormat;
  }

  public static <T> T parseContentType(final String accept, final T defaultFormat, final Function<String, T> parseFormat) {
    if (StringUtil.isEmpty(accept)) return defaultFormat;

    T format = parseFormat.apply(accept);
    if (format != null) return format;

    int eof = accept.indexOf(';');
    if (eof < 0) eof = accept.length();

    final String type = accept.substring(0, eof);
    format = parseFormat.apply(type.trim());
    return format != null ? format : defaultFormat;
  }

  public static DataFormat parseTypeToDataFormat(final String type) {
    return switch (type) {
      case CONTENT_TYPE_APP_CBOR -> CborFormat.INSTANCE;
      case CONTENT_TYPE_APP_JSON -> JsonFormat.INSTANCE;
      case CONTENT_TYPE_APP_XML, CONTENT_TYPE_TEXT_XML -> XmlFormat.INSTANCE;
      default -> null;
    };
  }

  // ====================================================================================================
  //  Message util
  // ====================================================================================================
  public static Message newDataMessage(final Map<String, String> metadata, final Object data) {
    return new ObjectMessage(metadata, data);
  }

  public static Message newDataMessage(final MessageMetadata metadata, final Object data) {
    return new ObjectMessage(metadata, data);
  }

  public static Message newRawMessage(final Map<String, String> metadata, final byte[] content) {
    return new RawMessage(metadata, content);
  }

  public static Message newRawMessage(final MessageMetadata metadata, final byte[] content) {
    return new RawMessage(metadata, content);
  }

  public static Message newRawMessage(final Map<String, String> metadata, final String content) {
    return new RawMessage(metadata, content.getBytes(StandardCharsets.UTF_8));
  }

  public static Message newRawMessage(final MessageMetadata metadata, final String content) {
    return new RawMessage(metadata, content.getBytes(StandardCharsets.UTF_8));
  }

  public static Message newEmptyMessage(final Map<String, String> metadata) {
    return new EmptyMessage(metadata);
  }

  public static Message newEmptyMessage(final MessageMetadata metadata) {
    return new EmptyMessage(metadata);
  }

  public static Message newErrorMessage(final MessageError error) {
    return new ErrorMessage(error);
  }

  private static abstract class AbstractMessage implements Message {
    private final MessageMetadata metadata;
    private final long timestamp;

    protected AbstractMessage(final Map<String, String> metadata) {
      this(new MessageMetadataMap(metadata));
    }

    protected AbstractMessage(final MessageMetadata metadata) {
      this.metadata = metadata;
      this.timestamp = System.nanoTime();
    }

    @Override public Message retain() { return this; }
    @Override public Message release() { return this; }

    @Override
    public long timestampNs() {
      return timestamp;
    }

    @Override
    public MessageMetadata metadata() {
      return metadata;
    }

    @Override
    public int estimateSize() {
      final LongValue size = new LongValue();
      size.add(contentLength());
      metadata.forEach((key, val) -> size.add(8 + key.length() + val.length()));
      return size.intValue();
    }
  }

  public static final class RawMessage extends AbstractMessage {
    private final byte[] content;

    public RawMessage(final Map<String, String> metadata, final byte[] content) {
      super(metadata);
      this.content = content;
    }

    public RawMessage(final MessageMetadata metadata, final byte[] content) {
      super(metadata);
      this.content = content;
    }

    public byte[] content() {
      return content;
    }

    @Override
    public int contentLength() {
      return BytesUtil.length(content);
    }

    @Override
    public long writeContentToStream(final OutputStream stream) throws IOException {
      if (ArrayUtil.isEmpty(content)) return 0;

      stream.write(content);
      return content.length;
    }

    @Override public long writeContentToStream(final DataOutput stream) throws IOException {
      if (ArrayUtil.isEmpty(content)) return 0;

      stream.write(content);
      return content.length;
    }

    @Override
    public <T> T convertContent(final DataFormat format, final Class<T> classOfT) {
      return format.fromBytes(content, classOfT);
    }
  }

  public static final class ObjectMessage extends TypedMessage<Object> {
    private ObjectMessage(final Map<String, String> metadata, final Object data) {
      super(metadata, data);
    }

    private ObjectMessage(final MessageMetadata metadata, final Object data) {
      super(metadata, data);
    }
  }

  public static class TypedMessage<TData> extends AbstractMessage {
    private final TData data;

    public TypedMessage(final Map<String, String> metadata, final TData data) {
      super(metadata);
      this.data = data;
    }

    public TypedMessage(final MessageMetadata metadata, final TData data) {
      super(metadata);
      this.data = data;
    }

    public TData content() {
      return data;
    }

    @Override public int contentLength() { throw new UnsupportedOperationException(); }
    @Override public long writeContentToStream(final OutputStream stream) { throw new UnsupportedOperationException(); }
    @Override public long writeContentToStream(final DataOutput stream) { throw new UnsupportedOperationException(); }

    @Override
    public <T> T convertContent(final DataFormat format, final Class<T> classOfT) {
      return format.convert(data, classOfT);
    }
  }

  public static final class ErrorMessage extends AbstractMessage {
    private final MessageError error;

    public ErrorMessage(final MessageError error) {
      super(EmptyMetadata.INSTANCE);
      this.error = error;
    }

    public MessageError error() {
      return error;
    }

    @Override public int contentLength() { throw new UnsupportedOperationException(); }
    @Override public long writeContentToStream(final OutputStream stream) { throw new UnsupportedOperationException(); }
    @Override public long writeContentToStream(final DataOutput stream) { throw new UnsupportedOperationException(); }

    @Override
    public <T> T convertContent(final DataFormat format, final Class<T> classOfT) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class EmptyMessage extends AbstractMessage {
    private EmptyMessage(final Map<String, String> metadata) {
      super(metadata);
    }

    private EmptyMessage(final MessageMetadata metadata) {
      super(metadata);
    }

    @Override public int contentLength() { return 0; }
    @Override public long writeContentToStream(final OutputStream stream) { return 0; }
    @Override public long writeContentToStream(final DataOutput stream) { return 0; }
    @Override public <T> T convertContent(final DataFormat format, final Class<T> classOfT) { return null; }
  }

  public static final class EmptyMetadata implements MessageMetadata {
    public static final EmptyMetadata INSTANCE = new EmptyMetadata();

    private EmptyMetadata() {
      // no-op
    }

    @Override public boolean isEmpty() { return true; }
    @Override public int size() { return 0; }

    @Override public String get(final String key) { return null; }
    @Override public List<String> getList(final String key) { return null; }

    @Override public void forEach(final BiConsumer<? super String, ? super String> action) { }
  }
}
