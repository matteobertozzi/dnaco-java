package tech.dnaco.dispatcher.message;

import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;

public class MessageUtil {
  private static final IntEncoder INT_ENCODER = IntEncoder.BIG_ENDIAN;
  private static final IntDecoder INT_DECODER = IntDecoder.BIG_ENDIAN;
/*
  private MessageUtil() {
    // no-op
  }

  public static class DnacoMessage {

    public DnacoMessage(final long packetId, final int metaCount, final ByteArraySlice metadata, final ByteArraySlice body) {
    }

  }

  //
  // +---+----+----+-----+----------------+---------------+-----------+
  // | - | 11 | 11 | 111 | metadata-count | metadata-size | packet-id |
  // +---+----+----+-----+----------------+---------------+-----------+
  // | metadata ...                                                   |
  // +----------------------------------------------------------------+
  // | data ...                                                       |
  // +----------------------------------------------------------------+
  public static DnacoMessage decodeMessage(final DnacoFrame frame) {
    final ByteArraySlice buf = frame.getData();
    int bufOff = 0;

    final int head = buf.get(bufOff++) & 0xff;
    final int metaCountBytes = ((head >> 5) & 3);
    final int metaSizeBytes = 1 + ((head >> 3) & 3);
    final int pktIdBytes = 1 + (head & 7);

    int metaCount = 0;
    int metaSize = 0;
    if (metaCountBytes > 0) {
      metaCount = Math.toIntExact(INT_DECODER.readFixed(buf, bufOff, metaCountBytes)); bufOff += metaCountBytes;
      metaSize = Math.toIntExact(INT_DECODER.readFixed(buf, bufOff, metaSizeBytes)); bufOff += metaSizeBytes;
    }

    final long packetId = INT_DECODER.readFixed(buf, bufOff, pktIdBytes); bufOff += pktIdBytes;
    final ByteArraySlice metadata = metaSize > 0 ? new ByteArraySlice(buf.rawBuffer(), bufOff, metaSize) : null;
    bufOff += metaSize;
    final ByteArraySlice body = new ByteArraySlice(buf.rawBuffer(), bufOff, buf.length() - bufOff)
    return new DnacoMessage(packetId, metaCount, metadata, body);
  }

  public static DnacoFrame encodeMessage(final DnacoMessage message) {
    final ByteBuf data = message.data();

    final ByteBuf frame = PooledByteBufAllocator.DEFAULT.buffer();
    if (message.hasMetadata()) {
      final ByteBuf metadata = PooledByteBufAllocator.DEFAULT.buffer();
      try {
        encodeMetadata(metadata, message.metadataMap());

        final int metaCountBytes = IntUtil.size(message.metadataCount());
        final int metaBytes = IntUtil.size(metadata.readableBytes());
        final int pktIdBytes = IntUtil.size(message.packetId());
        frame.writeByte((metaCountBytes << 5) | ((metaBytes - 1) << 3) | (pktIdBytes - 1));
        ByteBufIntUtil.writeFixed(frame, message.metadataCount(), metaCountBytes);
        ByteBufIntUtil.writeFixed(frame, metadata.readableBytes(), metaBytes);
        ByteBufIntUtil.writeFixed(frame, message.packetId(), pktIdBytes);
        frame.writeBytes(metadata);
        frame.writeBytes(data);
      } finally {
        metadata.release();
      }
    } else {
      final int pktIdBytes = IntUtil.size(message.packetId());
      frame.writeByte(pktIdBytes - 1);
      ByteBufIntUtil.writeFixed(frame, message.packetId(), pktIdBytes);
      frame.writeBytes(data);
    }

    return DnacoFrame.alloc(1, frame);
  }

  // ===============================================================================================
  //  Message Metadata related
  // ===============================================================================================
  //  metadata = {key1: val1, key2: val2, key3: [val3a, val3b]}
  // ===============================================================================================
  public static final String METADATA_FOR_HTTP_METHOD = ":method";
  public static final String METADATA_FOR_HTTP_URI = ":uri";
  public static final String METADATA_FOR_HTTP_STATUS = ":status";

  private static final int STD_KEYS_MAX = 64;
  private static final IndexedHashSet<String> STD_HEADERS_KEYS = new IndexedHashSet<>();
  static {
    STD_HEADERS_KEYS.add(METADATA_FOR_HTTP_METHOD);
    STD_HEADERS_KEYS.add(METADATA_FOR_HTTP_URI);
    STD_HEADERS_KEYS.add(METADATA_FOR_HTTP_STATUS);
    STD_HEADERS_KEYS.add("content-type");
    STD_HEADERS_KEYS.add("content-length");
  }

  public static boolean isMetaKeyReserved(final String key) {
    return key.startsWith(":");
  }

  public static DnacoMetadataMap decodeMetadata(final ByteBuf buffer, final int count) {
    final DnacoMetadataMap metadata = new DnacoMetadataMap(count);

    String prevKey = null;
    for (int i = 0; i < count; ++i) {
      final int head = buffer.readByte() & 0xff;
      final int keyLength = head >> 1;
      final int valLenBytes = 1 + (head & 1);

      final String key;
      if (keyLength >= STD_KEYS_MAX) {
        key = STD_HEADERS_KEYS.get(keyLength - STD_KEYS_MAX);
        prevKey = key;
      } else if (keyLength == 0) {
        key = prevKey;
      } else {
        key = buffer.readSlice(keyLength).toString(StandardCharsets.UTF_8);
        prevKey = key;
      }

      final int valLength = Math.toIntExact(ByteBufIntUtil.readFixed(buffer, valLenBytes));
      final String value = buffer.readSlice(valLength).toString(StandardCharsets.UTF_8);
      metadata.add(key, value);
    }

    return metadata;
  }

  public static void encodeMetadata(final ByteBuf buffer, final DnacoMetadataMap metadata) {
    final List<Entry<String, String>> entries = metadata.entries();
    entries.sort((a, b) -> {
      final int cmp = StringUtil.compare(a.getKey(), b.getKey());
      return cmp != 0 ? cmp : StringUtil.compare(a.getValue(), b.getValue());
    });

    String prevKey = null;
    for (final Entry<String, String> entry: entries) {
      final byte[] value = entry.getValue().getBytes(StandardCharsets.UTF_8);
      final int valLenBytes = (value.length <= 0xff) ? 1 : 2;
      final int tableIndex = STD_HEADERS_KEYS.get(entry.getKey());
      if (tableIndex >= 0) {
        buffer.writeByte(((STD_KEYS_MAX + tableIndex) << 1) | (valLenBytes - 1));
      } else if (StringUtil.equals(prevKey, entry.getKey())) {
        buffer.writeByte(valLenBytes - 1);
      } else {
        final byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
        buffer.writeByte((key.length << 1) | (valLenBytes - 1));
        buffer.writeBytes(key);
        prevKey = entry.getKey();
      }

      ByteBufIntUtil.writeFixed(buffer, value.length, valLenBytes);
      buffer.writeBytes(value);
    }
  }
*/
}
