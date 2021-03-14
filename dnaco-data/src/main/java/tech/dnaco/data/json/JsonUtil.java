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

package tech.dnaco.data.json;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.data.JsonFormat;
import tech.dnaco.logging.Logger;

public final class JsonUtil {
  private JsonUtil() {
    // no-op
  }

  public static <T> T fromJson(final File file, final Class<T> valueType) throws IOException {
    return JsonFormat.INSTANCE.fromFile(file, valueType);
  }

  public static <T> T fromJson(final InputStream stream, final Class<T> valueType) throws IOException {
    return JsonFormat.INSTANCE.fromStream(stream, valueType);
  }

  public static <T> T fromJson(final byte[] json, final Class<T> valueType) {
    return JsonFormat.INSTANCE.fromBytes(json, valueType);
  }

  public static <T> T fromJson(final String json, final Class<T> valueType) {
    return JsonFormat.INSTANCE.fromString(json, valueType);
  }

  public static <T> T fromJson(final JsonElement json, final Class<T> valueType) {
    return JsonFormat.INSTANCE.convert(json, valueType);
  }

  public static <T> T fromJson(final JsonNode json, final Class<T> classOfT) {
    try {
      return JsonFormat.INSTANCE.fromTreeNode(json, classOfT);
    } catch (final JsonProcessingException e) {
      Logger.warn(e, "failed to parse json: {}", json);
      return null;
    }
  }

  public static <T> T fromJson(final File file, final TypeReference<T> valueType) throws IOException {
    return JsonFormat.INSTANCE.fromFile(file, valueType);
  }

  public static String toJson(final Object obj) {
    return JsonFormat.INSTANCE.asString(obj);
  }

  public static String toJson(final Object src, final boolean prettyPrint) {
    if (prettyPrint) {
      return JsonFormat.INSTANCE.asPrettyPrintString(src);
    }
    return JsonFormat.INSTANCE.asString(src);
  }

  public static JsonElement toJsonTree(final Object obj) {
    return JsonFormat.INSTANCE.convert(obj, JsonElement.class);
  }


  // ================================================================================
  //  Json Element helpers
  // ================================================================================
  public static boolean isNull(final JsonElement elem) {
    return elem == null || elem.isJsonNull();
  }

  public static boolean isNull(final JsonNode elem) {
    return elem == null || elem.isNull();
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
  public static boolean isEmpty(final JsonNode elem) {
    return elem == null || elem.isNull() || elem.isEmpty();
  }

  public static boolean isEmpty(final JsonElement elem) {
    if (elem == null || elem.isJsonNull()) return true;
    if (elem.isJsonObject() && elem.getAsJsonObject().isEmpty()) return true;
    if (elem.isJsonArray() && elem.getAsJsonArray().isEmpty()) return true;
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

  public interface Jsonable {
    JsonElement toJson();
  }
}
