/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.storage.demo;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonElement;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.data.json.JsonUtil;

public final class EntityData {
  private EntityData() {
    // no-op
  }

  public static byte[] encodeFromObject(final EntityDataType type, final Object value) {
    switch (type) {
      case NULL: return encodeNull();
      case BOOL: return encodeBoolean(value);
      case INT: return encodeInt(value);
      case FLOAT: return encodeFloat(value);
      case BYTES: return encodeBytes(value);
      case STRING: return encodeString(value);
      case JSON_ARRAY: return encodeJsonArray(value);
      case JSON_OBJECT: return encodeJsonObject(value);
    }
    throw new UnsupportedOperationException("invalid type: " + type);
  }

  public static Object decodeToObject(final EntityDataType type, final byte[] value) {
    return decodeToObject(type, new ByteArraySlice(value));
  }

  public static Object decodeToObject(final EntityDataType type, final ByteArraySlice rawValue) {
    switch (type) {
      case NULL: return decodeNull(rawValue);
      case BOOL: return decodeBoolean(rawValue);
      case INT: return decodeInt(rawValue);
      case FLOAT: return decodeFloat(rawValue);
      case BYTES: return decodeBytes(rawValue);
      case STRING: return decodeString(rawValue);
      case JSON_ARRAY: return decodeJsonArray(rawValue);
      case JSON_OBJECT: return decodeJsonObject(rawValue);
    }
    throw new UnsupportedOperationException("invalid type: " + type);
  }

  public static byte[] encodeNull() {
    return new byte[] { (byte) (EntityDataType.NULL.ordinal() & 0xff) };
  }

  public static Object decodeNull(final ByteArraySlice rawValue) {
    if (rawValue.length() != 1 || rawValue.get(0) != EntityDataType.NULL.ordinal()) {
      throw new IllegalArgumentException("expected 1 byte with null type, got " + rawValue);
    }
    return null;
  }

  public static byte[] encodeBoolean(final Object value) {
    if (value == null) return encodeNull();
    return new byte[] {
      (byte) (EntityDataType.BOOL.ordinal() & 0xff),
      (byte) ((Boolean) value ? 1 : 0)
    };
  }

  public static Object decodeBoolean(final ByteArraySlice rawValue) {
    if (rawValue.get(0) == EntityDataType.BOOL.ordinal()) {
      return rawValue.get(1) == 1;
    } else if (rawValue.get(0) == EntityDataType.NULL.ordinal()) {
      return null;
    }
    throw new IllegalArgumentException("expected NULL or BOOL, got " + rawValue.get(0) + ": " + rawValue);
  }

  public static byte[] encodeInt(final Object value) {
    if (value == null) return encodeNull();

    final byte[] buf;
    if (value instanceof Long) {
      buf = new byte[9];
      buf[0] = (byte) (EntityDataType.INT.ordinal() & 0xff);
      IntEncoder.BIG_ENDIAN.writeFixed64(buf, 1, (long)value);
    } else if (value instanceof Integer) {
      buf = new byte[5];
      buf[0] = (byte) (EntityDataType.INT.ordinal() & 0xff);
      IntEncoder.BIG_ENDIAN.writeFixed32(buf, 1, (int)value);
    } else if (value instanceof Double) {
      return encodeInt(Math.round((Double)value));
    } else if (value instanceof Float) {
      return encodeInt(Math.round((Float)value));
    } else {
      throw new IllegalArgumentException("expected Integer/Long got " + value.getClass() + ": " + value);
    }
    return buf;
  }

  public static Object decodeInt(final ByteArraySlice rawValue) {
    if (rawValue.get(0) == EntityDataType.INT.ordinal()) {
      if (rawValue.length() == 5) {
        return IntDecoder.BIG_ENDIAN.readFixed32(rawValue.rawBuffer(), rawValue.offset() + 1);
      }
      return IntDecoder.BIG_ENDIAN.readFixed64(rawValue.rawBuffer(), rawValue.offset() + 1);
    } else if (rawValue.get(0) == EntityDataType.NULL.ordinal()) {
      return null;
    }
    throw new IllegalArgumentException("expected NULL or INT, got " + rawValue.get(0) + ": " + rawValue);
  }

  public static byte[] encodeFloat(final Object value) {
    if (value == null) return encodeNull();

    final byte[] buf;
    if (value instanceof Double) {
      final long v = Double.doubleToLongBits((double)value);
      buf = new byte[9];
      buf[0] = (byte) (EntityDataType.FLOAT.ordinal() & 0xff);
      IntEncoder.BIG_ENDIAN.writeFixed64(buf, 1, v);
    } else if (value instanceof Float) {
      final int v = Float.floatToIntBits((float)value);
      buf = new byte[5];
      buf[0] = (byte) (EntityDataType.FLOAT.ordinal() & 0xff);
      IntEncoder.BIG_ENDIAN.writeFixed32(buf, 1, v);
    } else if (value instanceof Long || value instanceof Integer) {
      return encodeInt(value);
    } else {
      throw new IllegalArgumentException("expected Double/Float got " + value.getClass() + ": " + value);
    }
    return buf;
  }

  public static Object decodeFloat(final ByteArraySlice rawValue) {
    if (rawValue.get(0) == EntityDataType.FLOAT.ordinal()) {
      if (rawValue.length() == 5) {
        final int v = IntDecoder.BIG_ENDIAN.readFixed32(rawValue.rawBuffer(), rawValue.offset() + 1);
        return Float.intBitsToFloat(v);
      } else {
        final long v = IntDecoder.BIG_ENDIAN.readFixed64(rawValue.rawBuffer(), rawValue.offset() + 1);
        return Double.longBitsToDouble(v);
      }
    } else if (rawValue.get(0) == EntityDataType.INT.ordinal()) {
      return decodeInt(rawValue);
    } else if (rawValue.get(0) == EntityDataType.NULL.ordinal()) {
      return null;
    }
    throw new IllegalArgumentException("expected NULL or FLOAT, got " + rawValue.get(0) + ": " + rawValue);
  }

  public static byte[] encodeBytes(final Object value) {
    if (value == null) return encodeNull();

    final byte[] v = (byte[]) value;
    final byte[] buf = new byte[1 + v.length];
    buf[0] = (byte) (EntityDataType.BYTES.ordinal() & 0xff);
    System.arraycopy(v, 0, buf, 1, v.length);
    return buf;
  }

  public static Object decodeBytes(final ByteArraySlice rawValue) {
    if (rawValue.get(0) == EntityDataType.BYTES.ordinal()) {
      final byte[] buf = new byte[rawValue.length() - 1];
      for (int i = 0; i < buf.length; ++i) {
        buf[i] = (byte) rawValue.get(i + 1);
      }
      return buf;
    } else if (rawValue.get(0) == EntityDataType.NULL.ordinal()) {
      return null;
    }
    throw new IllegalArgumentException("expected NULL or BYTES, got " + rawValue.get(0) + " " + EntityDataType.values()[rawValue.get(0)] + ": " + rawValue);
  }

  public static byte[] encodeString(final Object value) {
    if (value == null) return encodeNull();

    final byte[] v = ((String) value).getBytes();
    final byte[] buf = new byte[1 + v.length];
    buf[0] = (byte) (EntityDataType.STRING.ordinal() & 0xff);
    System.arraycopy(v, 0, buf, 1, v.length);
    return buf;
  }

  public static Object decodeString(final ByteArraySlice rawValue) {
    if (rawValue.get(0) == EntityDataType.STRING.ordinal()) {
      final byte[] buf = new byte[rawValue.length() - 1];
      for (int i = 0; i < buf.length; ++i) {
        buf[i] = (byte) rawValue.get(i + 1);
      }
      return new String(buf);
    } else if (rawValue.get(0) == EntityDataType.NULL.ordinal()) {
      return null;
    }
    throw new IllegalArgumentException("expected NULL or STRING, got " + rawValue.get(0) + ": " + rawValue);
  }

  public static byte[] encodeJsonArray(final Object value) {
    if (value == null) return encodeNull();

    final JsonArray json;
    if (value instanceof JsonArray) {
      json = (JsonArray) value;
    } else if (value instanceof JsonElement) {
      json = ((JsonElement)value).getAsJsonArray();
    } else if (value instanceof String) {
      return encodeString(value);
    } else {
      throw new IllegalArgumentException("expected JsonElement/JsonArray, got " + value.getClass() + ": " + value);
    }
    return encodeString(json.toString());
  }

  public static Object decodeJsonArray(final ByteArraySlice rawValue) {
    if (rawValue.get(0) == EntityDataType.JSON_ARRAY.ordinal()) {
      throw new UnsupportedOperationException();
    } else if (rawValue.get(0) == EntityDataType.STRING.ordinal()) {
      final String json = (String) decodeString(rawValue);
      return JsonUtil.fromJson(json, JsonArray.class);
    } else if (rawValue.get(0) == EntityDataType.NULL.ordinal()) {
      return null;
    }
    throw new IllegalArgumentException("expected NULL or JSON_ARRAY, got " + rawValue.get(0) + ": " + rawValue);
  }

  public static byte[] encodeJsonObject(final Object value) {
    if (value == null) return encodeNull();

    final JsonObject json;
    if (value instanceof JsonObject) {
      json = (JsonObject)value;
    } else if (value instanceof JsonElement) {
      json = ((JsonElement)value).getAsJsonObject();
    } else if (value instanceof String) {
      return encodeString(value);
    } else {
      throw new IllegalArgumentException("expected JsonElement/JsonArray, got " + value.getClass() + ": " + value);
    }
    return encodeString(json.toString());
  }

  public static Object decodeJsonObject(final ByteArraySlice rawValue) {
    if (rawValue.get(0) == EntityDataType.JSON_OBJECT.ordinal()) {
      throw new UnsupportedOperationException();
    } else if (rawValue.get(0) == EntityDataType.STRING.ordinal()) {
      final String json = (String) decodeString(rawValue);
      return JsonUtil.fromJson(json, JsonObject.class);
    } else if (rawValue.get(0) == EntityDataType.NULL.ordinal()) {
      return null;
    }
    throw new IllegalArgumentException("expected NULL or JSON_OBJECT, got " + rawValue.get(0) + ": " + rawValue);
  }
}
