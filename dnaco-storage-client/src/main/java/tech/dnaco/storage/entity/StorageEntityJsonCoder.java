package tech.dnaco.storage.entity;

import java.util.Base64;
import java.util.HashSet;

import com.google.gson.JsonObject;

import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.JsonUtil;

public class StorageEntityJsonCoder implements StorageEntityEncoder, StorageEntityDecoder {
  private final JsonObject json;

  public StorageEntityJsonCoder() {
    this(new JsonObject());
  }

  public StorageEntityJsonCoder(final JsonObject value) {
    this.json = value;
  }

  public void reset() {
    for (final String key: new HashSet<>(json.keySet())) {
      json.remove(key);
    }
  }

  @Override
  public void addKeyField(final String name, final int index, final Class<?> classType, final Object value) {
    addField(name, classType, value);
  }

  @Override
  public void addValueField(final String name, final Class<?> classType, final Object value) {
    addField(name, classType, value);
  }

  private void addField(final String name, final Class<?> classType, final Object value) {
    if (classType == boolean.class) {
      json.addProperty(name, (boolean)value);
    } else if (classType == int.class) {
      json.addProperty(name, (int)value);
    } else if (classType == long.class) {
      json.addProperty(name, (long)value);
    } else if (classType == String.class) {
      json.addProperty(name, (String)value);
    } else if (classType == float.class) {
      json.addProperty(name, (float)value);
    } else if (classType == double.class) {
      json.addProperty(name, (double)value);
    } else if (classType == byte[].class) {
      json.addProperty(name, Base64.getEncoder().encodeToString((byte[])value));
    } else {
      json.add(name, JsonUtil.toJsonTree(value));
    }
  }

  @Override
  public Object getKeyField(final String name, final int index, final Class<?> classType) {
    return getField(name, classType);
  }

  @Override
  public Object getValueField(final String name, final Class<?> classType) {
    return getField(name, classType);
  }

  private Object getField(final String name, final Class<?> classType) {
    if (classType == boolean.class) {
      return JsonUtil.getBoolean(json, name, false);
    } else if (classType == int.class) {
      return JsonUtil.getInt(json, name, 0);
    } else if (classType == long.class) {
      return JsonUtil.getLong(json, name, 0);
    } else if (classType == String.class) {
      return JsonUtil.getString(json, name, null);
    } else if (classType == float.class) {
      return JsonUtil.getFloat(json, name, 0);
    } else if (classType == double.class) {
      return JsonUtil.getDouble(json, name, 0);
    } else if (classType == byte[].class) {
      final String b64 = JsonUtil.getString(json, name, null);
      return StringUtil.isEmpty(b64) ? null : Base64.getDecoder().decode(b64);
    } else {
      return JsonUtil.fromJson(json.get(name), classType);
    }
  }

  public JsonObject toJsonObject() {
    return json;
  }

  public String toJson() {
    return json.toString();
  }
}