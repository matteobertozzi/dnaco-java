package tech.dnaco.storage.entity;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import tech.dnaco.util.JsonUtil;

public class StorageEntityKeyValueCoder {
  public static final class StorageEntityKeyValueEncoder implements StorageEntityEncoder, AutoCloseable {
    private final ByteBuf keyPrefix = PooledByteBufAllocator.DEFAULT.buffer();
    private final ArrayList<StorageKeyValue> keyValues = new ArrayList<>();

    public void close() {
      keyPrefix.release();
    }

    public List<StorageKeyValue> getKeyValues() {
      return keyValues;
    }

    @Override
    public void addKeyField(String name, int index, Class<?> classType, Object value) {
      if (value == null) throw new IllegalArgumentException("key component '" + name + "' cannot be null");

      keyPrefix.writeByte('/');
      if (classType == String.class) {
        keyPrefix.writeBytes(((String)value).getBytes(StandardCharsets.UTF_8));
      } else if (classType == int.class) {
        keyPrefix.writeBytes(String.format("%010d", value).getBytes(StandardCharsets.US_ASCII));
      } else if (classType == long.class) {
        keyPrefix.writeBytes(String.format("%020d", value).getBytes(StandardCharsets.US_ASCII));
      } else {
        throw new UnsupportedOperationException("unsupported key type " + classType);
      }
    }

    @Override
    public void addValueField(String name, Class<?> classType, Object value) {
      final ByteBuf keyBuf = PooledByteBufAllocator.DEFAULT.buffer();
      keyBuf.writeBytes(keyPrefix.slice()).writeByte('/').writeBytes(name.getBytes(StandardCharsets.UTF_8));

      final ByteBuf valueBuf = PooledByteBufAllocator.DEFAULT.buffer();
      if (classType == boolean.class) {
        valueBuf.writeByte((boolean)value ? 1 : 0);
      } else if (classType == int.class) {
        valueBuf.writeInt((int)value);
      } else if (classType == long.class) {
        valueBuf.writeLong((long)value);
      } else if (classType == String.class) {
        valueBuf.writeBytes(((String)value).getBytes(StandardCharsets.UTF_8));
      } else if (classType == float.class) {
        valueBuf.writeFloat((float)value);
      } else if (classType == double.class) {
        valueBuf.writeDouble((double)value);
      } else if (classType == byte[].class) {
        valueBuf.writeBytes((byte[])value);
      } else {
        valueBuf.writeBytes(JsonUtil.toJson(value).getBytes(StandardCharsets.UTF_8));
      }

      keyValues.add(new StorageKeyValue(keyBuf, valueBuf));
    }
  }

  public static final class StorageEntityKeyValueDecoder implements StorageEntityDecoder {
    private final HashMap<String, ByteBuf> data;
    private final String[] key;
    private final int fieldNameIndex;

    public StorageEntityKeyValueDecoder(final List<StorageKeyValue> keyValues) {
      final ByteBuf refKey = keyValues.get(0).getKey();

      this.data = new HashMap<>(keyValues.size());
      this.fieldNameIndex = ByteBufUtil.indexOf(refKey, refKey.readableBytes(), 0, (byte) '/');
      this.key = refKey.slice(1, fieldNameIndex).toString(StandardCharsets.UTF_8).split("/");

      for (StorageKeyValue kv: keyValues) {
        final ByteBuf keyBuf = kv.getKey();
        final int keyLen = keyBuf.readableBytes();
        final int index = ByteBufUtil.indexOf(keyBuf, keyLen, 0, (byte) '/');
        if (index != fieldNameIndex) {
          throw new IllegalArgumentException("key/value prefixes are not matching: " + index);
        }

        final String field = keyBuf.slice(index + 1, keyLen - index - 1).toString(StandardCharsets.UTF_8);
        System.out.println(" ---> " + field + ":" + field.length() + " -> " + kv.getValue());
        data.put(field, kv.getValue());
      }
    }

    @Override
    public Object getKeyField(String name, int index, Class<?> classType) {
      if (classType == String.class) {
        return key[index];
      } else if (classType == int.class) {
        return Integer.parseInt(key[index]);
      } else if (classType == long.class) {
        return Long.parseLong(key[index]);
      } else {
        throw new UnsupportedOperationException("unsupported key type " + classType);
      }
    }

    @Override
    public Object getValueField(String name, Class<?> classType) {
      final ByteBuf value = data.get(name);

      for (Entry<String, ByteBuf> entry: data.entrySet()) {
        System.out.println(name + " -> "  + entry.getKey() + " -> " + entry.getKey().equals(name) + " -> " + entry.getValue());
      }

      if (value == null) {
        System.out.println("null field " + name + " -> " + value + " -> " + data.keySet());
        return null;
      } else if (classType == boolean.class) {
        return value.readByte() != 0;
      } else if (classType == int.class) {
        return value.readInt();
      } else if (classType == long.class) {
        return value.readLong();
      } else if (classType == String.class) {
        return value.toString(StandardCharsets.UTF_8);
      } else if (classType == float.class) {
        return value.readFloat();
      } else if (classType == double.class) {
        return value.readDouble();
      } else if (classType == byte[].class) {
        byte[] buf = new byte[value.readableBytes()];
        value.readBytes(buf);
        return buf;
      } else {
        return JsonUtil.fromJson(value.toString(StandardCharsets.UTF_8), classType);
      }
    }
  }
}