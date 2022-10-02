package tech.dnaco.dispatcher.message;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.LongValue;
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
    final String accept = metadata.getString(METADATA_ACCEPT, null);
    if (StringUtil.isEmpty(accept)) return JsonFormat.INSTANCE;

    DataFormat format = parseAcceptFormat(accept);
    if (format != null) return format;

    int lastIndex = 0;
    while (lastIndex < accept.length()) {
      int eof = accept.indexOf(',', lastIndex);
      if (eof < 0) eof = accept.length();

      String type = accept.substring(lastIndex, eof);
      final int qIndex = type.lastIndexOf(';');
      if (qIndex > 0) type = type.substring(0, qIndex);

      format = parseAcceptFormat(type);
      if (format != null) return format;
      lastIndex = eof + 1;
    }
    return JsonFormat.INSTANCE;
  }

  private static DataFormat parseAcceptFormat(final String type) {
    return switch (type) {
      case CONTENT_TYPE_APP_XML, CONTENT_TYPE_TEXT_XML -> XmlFormat.INSTANCE;
      case CONTENT_TYPE_APP_CBOR -> CborFormat.INSTANCE;
      case CONTENT_TYPE_APP_JSON -> JsonFormat.INSTANCE;
      default -> null;
    };
  }

  // ====================================================================================================
  //  Message util
  // ====================================================================================================
  public static Message newMessage(final Map<String, String> metadata, final Object data) {
    return new ObjectMessage(metadata, data);
  }

  public static Message newMessage(final MessageMetadata metadata, final Object data) {
    return new ObjectMessage(metadata, data);
  }

  public static Message newMessage(final Map<String, String> metadata, final byte[] content) {
    return new RawMessage(metadata, content);
  }

  public static Message newMessage(final MessageMetadata metadata, final byte[] content) {
    return new RawMessage(metadata, content);
  }

  public static Message newEmptyMessage(final Map<String, String> metadata) {
    return new EmptyMessage(metadata);
  }

  public static Message newEmptyMessage(final MessageMetadata metadata) {
    return new EmptyMessage(metadata);
  }

  private static abstract class AbstractMessage implements Message {
    private final MessageMetadata metadata;

    protected AbstractMessage(final Map<String, String> metadata) {
      this.metadata = new MessageMetadataMap(metadata);
    }

    protected AbstractMessage(final MessageMetadata metadata) {
      this.metadata = metadata;
    }

    @Override public Message retain() { return this; }
    @Override public Message release() { return this; }

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
      if (content == null) return 0;
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

    @Override
    public <T> T convertContent(final DataFormat format, final Class<T> classOfT) {
      return format.convert(data, classOfT);
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
