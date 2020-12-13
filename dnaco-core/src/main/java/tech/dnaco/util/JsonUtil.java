/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.collections.HashIndexedArray;
import tech.dnaco.collections.HashIndexedArrayMap;

public final class JsonUtil {
  public static final String JSON_DATE_FORMAT_PATTERN = "YYYYMMddHHmmss";
  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(byte[].class, new BytesTypeAdapter())
      .registerTypeAdapter(HashIndexedArray.class, new HashIndexedArrayTypeAdapter())
      .registerTypeAdapter(HashIndexedArrayMap.class, new HashIndexedArrayMapTypeAdapter())
      .setDateFormat(JSON_DATE_FORMAT_PATTERN)
      .disableHtmlEscaping()
      .create();

  @Retention(RetentionPolicy.SOURCE)
  @Target(ElementType.TYPE)
  @interface JsonEntityModel {
  }

  private JsonUtil() {
    // no-op
  }

  public static Gson getGson() {
    return GSON;
  }

  // ================================================================================
  // From Json helpers
  // ================================================================================
  public static <T> T fromJson(final String json, final Class<T> classOfT) {
    return GSON.fromJson(json, classOfT);
  }

  public static <T> T fromJson(final JsonElement json, final Class<T> classOfT) {
    return GSON.fromJson(json, classOfT);
  }

  public static <T> T fromJson(final byte[] json, final Class<T> classOfT) throws IOException {
    return fromJson(json, 0, BytesUtil.length(json), classOfT);
  }

  public static <T> T fromJson(final byte[] json, final int jsonOff, final int jsonLen, final Class<T> classOfT)
      throws IOException {
    if (jsonLen == 0) return null;
    try (ByteArrayInputStream stream = new ByteArrayInputStream(json, jsonOff, jsonLen)) {
      try (InputStreamReader reader = new InputStreamReader(stream)) {
        return GSON.fromJson(reader, classOfT);
      }
    }
  }

  public static <T> T fromJson(final File jsonFile, final Class<T> classOfT)
      throws IOException {
    try (FileReader reader = new FileReader(jsonFile)) {
      return GSON.fromJson(reader, classOfT);
    }
  }

  public static <T> T fromJson(final InputStreamReader stream, final Class<T> classOfT)
      throws IOException {
    try (JsonReader reader = new JsonReader(stream)) {
      return GSON.fromJson(reader, classOfT);
    }
  }

  public static <K,V> Map<K,V> hashMapFromJson(final String json,
      final Class<K> kClass, final Class<V> vClass) {
    final Type type = TypeToken.getParameterized(HashMap.class, kClass, vClass).getType();
    return GSON.fromJson(json, type);
  }

  public static <K,V> Map<K,V> hashMapFromJson(final JsonObject json,
      final Class<K> kClass, final Class<V> vClass) {
    final Type type = TypeToken.getParameterized(HashMap.class, kClass, vClass).getType();
    return GSON.fromJson(json, type);
  }

  public static <T> List<T> listFromJson(final String json, final Class<T> clazz) {
    final Type type = TypeToken.getParameterized(ArrayList.class, clazz).getType();
    return GSON.fromJson(json, type);
  }

  // ================================================================================
  //  To Json helpers
  // ================================================================================
  public static String toJson(final Object src) {
    return GSON.toJson(src);
  }

  public static JsonElement toJsonTree(final Object src) {
    return GSON.toJsonTree(src);
  }

  // ================================================================================
  //  Json Element helpers
  // ================================================================================
  public static boolean isNull(final JsonElement elem) {
    return elem == null || elem.isJsonNull();
  }

  // ================================================================================
  //  Json Array helpers
  // ================================================================================
  public static JsonArray newJsonArray(final int[] buf) {
    return newJsonArray(buf, 0, ArrayUtil.length(buf));
  }

  public static JsonArray newJsonArray(final int[] buf, final int off, final int len) {
    final JsonArray json = new JsonArray();
    for (int i = 0; i < len; ++i) {
      json.add(buf[off + i]);
    }
    return json;
  }

  public static JsonArray newJsonArray(final long[] buf) {
    return newJsonArray(buf, 0, ArrayUtil.length(buf));
  }

  public static JsonArray newJsonArray(final long[] buf, final int off, final int len) {
    final JsonArray json = new JsonArray();
    for (int i = 0; i < len; ++i) {
      json.add(buf[off + i]);
    }
    return json;
  }

  public static JsonArray newJsonArray(final String[] buf) {
    return newJsonArray(buf, 0, ArrayUtil.length(buf));
  }

  public static JsonArray newJsonArray(final String[] buf, final int off, final int len) {
    final JsonArray json = new JsonArray();
    for (int i = 0; i < len; ++i) {
      json.add(buf[off + i]);
    }
    return json;
  }

  // ================================================================================
  //  Json Object helpers
  // ================================================================================
  public static JsonObject newJsonObject(final String key, final String value) {
    final JsonObject json = new JsonObject();
    json.addProperty(key, value);
    return json;
  }

  public static JsonObject newJsonObject(final String key, final Object value) {
    return newJsonObject(key, JsonUtil.toJsonTree(value));
  }

  public static JsonObject newJsonObject(final String key, final JsonElement value) {
    final JsonObject json = new JsonObject();
    json.add(key, value);
    return json;
  }

  // ================================================================================
  //  Json Object helpers
  // ================================================================================
  public static boolean isEmpty(final JsonElement elem) {
    if (elem == null || elem.isJsonNull()) return true;
    if (elem.isJsonObject() && elem.getAsJsonObject().size() == 0) return true;
    if (elem.isJsonArray() && elem.getAsJsonArray().size() == 0) return true;
    return false;
  }

  public static boolean isEmpty(final JsonObject obj) {
    return obj == null || obj.size() == 0;
  }

  public static boolean isNotEmpty(final JsonObject obj) {
    return obj != null && obj.size() > 0;
  }

  public static boolean getBoolean(final JsonObject json, final String memberName, final boolean defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element.getAsBoolean();
  }

  public static int getInt(final JsonObject json, final String memberName, final int defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element.getAsInt();
  }

  public static long getLong(final JsonObject json, final String memberName, final long defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element.getAsLong();
  }

  public static float getFloat(final JsonObject json, final String memberName, final float defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element.getAsFloat();
  }

  public static double getDouble(final JsonObject json, final String memberName, final double defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element.getAsDouble();
  }

  public static String getString(final JsonObject json, final String memberName, final String defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element.getAsString();
  }

  public static JsonElement getJsonElement(final JsonObject json, final String memberName, final JsonElement defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element;
  }

  public static JsonObject getJsonObject(final JsonObject json, final String memberName, final JsonObject defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element.getAsJsonObject();
  }

  public static JsonArray getJsonArray(final JsonObject json, final String memberName, final JsonArray defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element.getAsJsonArray();
  }

  public static JsonPrimitive getJsonPrimitive(final JsonObject json, final String memberName, final JsonPrimitive defaultValue) {
    final JsonElement element = json.get(memberName);
    return (element == null || element.isJsonNull()) ? defaultValue : element.getAsJsonPrimitive();
  }

  // ================================================================================
  // Gson Type Adapters
  // ================================================================================
  public static final class BytesTypeAdapter implements JsonDeserializer<byte[]>, JsonSerializer<byte[]> {
    @Override
    public JsonElement serialize(final byte[] src, final Type typeOfSrc, final JsonSerializationContext context) {
      return new JsonPrimitive(Base64.getEncoder().encodeToString(src));
    }

    @Override
    public byte[] deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context)
        throws JsonParseException {
      return Base64.getDecoder().decode(json.getAsString());
    }
  }

  public static final class HashIndexedArrayTypeAdapter implements JsonSerializer<HashIndexedArray<?>>, JsonDeserializer<HashIndexedArray<?>> {
    @Override
    public JsonElement serialize(final HashIndexedArray<?> src, final Type typeOfSrc, final JsonSerializationContext context) {
      final JsonArray json = new JsonArray();
      for (int i = 0, n = src.size(); i < n; ++i) {
        json.add(toJsonTree(src.get(i)));
      }
      return json;
    }

    @Override
    public HashIndexedArray<?> deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context)
        throws JsonParseException {
      final JsonArray jsonArray = json.getAsJsonArray();
      if (jsonArray.get(0).isJsonPrimitive()) {
        final JsonPrimitive primitive = jsonArray.get(0).getAsJsonPrimitive();
        if (primitive.isString()) return new HashIndexedArray<>(JsonUtil.fromJson(jsonArray, String[].class));
      }
      throw new UnsupportedOperationException();
    }
  }

  public static final class HashIndexedArrayMapTypeAdapter implements JsonSerializer<HashIndexedArrayMap<?, ?>> {
    @Override
    public JsonElement serialize(final HashIndexedArrayMap<?, ?> src, final Type typeOfSrc, final JsonSerializationContext context) {
      final HashIndexedArray<?> keys = src.getKeyIndex();
      final JsonObject json = new JsonObject();
      for (int i = 0, n = src.size(); i < n; ++i) {
        json.add(String.valueOf(keys.get(i)), toJsonTree(src.get(i)));
      }
      return json;
    }
  }

  public static class Test {
    final HashIndexedArray<String> h = new HashIndexedArray<>(new String[] { "a", "b", "c" });

    public String toString() {
      return h.toString();
    }
  }

  public static void main(final String[] args) {
    final Test t = new Test();
    final String json = JsonUtil.toJson(t);
    System.out.println(json);
    System.out.println(JsonUtil.fromJson(json, Test.class));
  }
}
