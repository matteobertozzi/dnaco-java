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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import tech.dnaco.bytes.ByteArrayReader;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.collections.HashIndexedArray;
import tech.dnaco.collections.HashIndexedArrayMap;
import tech.dnaco.data.DataFormatUtil;
import tech.dnaco.data.JsonFormatUtil.JsonArray;
import tech.dnaco.data.JsonFormatUtil.JsonElement;
import tech.dnaco.data.JsonFormatUtil.JsonObject;
import tech.dnaco.data.JsonFormatUtil.JsonPrimitive;
import tech.dnaco.io.BytesInputStream;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.StringUtil;

public final class JsonUtil {
  public static final String JSON_DATE_FORMAT_PATTERN = "YYYYMMddHHmmss";

  @Retention(RetentionPolicy.SOURCE)
  @Target(ElementType.TYPE)
  @interface JsonEntityModel {
  }

  private JsonUtil() {
    // no-op
  }

  // ================================================================================
  // From Json helpers
  // ================================================================================
  public static <T> T fromJson(final String json, final Class<T> classOfT) {
    try {
      return StringUtil.isNotEmpty(json) ? DataFormatUtil.json().fromString(json, classOfT) : null;
    } catch (final IOException e) {
      Logger.error(e, "invalid json: {}", json);
      return null;
    }
  }

  public static <T> T fromJson(final JsonElement json, final Class<T> classOfT) {
    try {
      return json != null ? DataFormatUtil.json().fromTreeNode(json.toTreeNode(), classOfT) : null;
    } catch (final IOException e) {
      Logger.error(e, "invalid json: {} {}", json, classOfT);
      return null;
    }
  }

  public static <T> T fromJson(final JsonNode json, final Class<T> classOfT) {
    try {
      return DataFormatUtil.json().fromTreeNode(json, classOfT);
    } catch (final IOException e) {
      Logger.error(e, "failed to parse json: {}", json);
      return null;
    }
  }

  public static <T> T fromJson(final byte[] json, final Class<T> classOfT) {
    return fromJson(json, 0, BytesUtil.length(json), classOfT);
  }

  public static <T> T fromJson(final byte[] json, final int jsonOff, final int jsonLen, final Class<T> classOfT) {
    if (jsonLen == 0) return null;
    try (BytesInputStream stream = new ByteArrayReader(json, jsonOff, jsonLen)) {
      return DataFormatUtil.json().fromStream(stream, classOfT);
    } catch (final IOException e) {
      Logger.error(e, "failed to read json");
      return null;
    }
  }

  public static <T> T fromJson(final File jsonFile, final Class<T> classOfT) throws IOException {
    return DataFormatUtil.json().fromFile(jsonFile, classOfT);
  }

  public static <T> T fromJson(final InputStream stream, final Class<T> classOfT) throws IOException {
    return DataFormatUtil.json().fromStream(stream, classOfT);
  }

  public static <T> T fromJson(final File file, final TypeReference<T> valueType) throws IOException {
    return DataFormatUtil.json().fromFile(file, valueType);
  }

  // ================================================================================
  //  To Json helpers
  // ================================================================================
  public static String toJson(final Object src) {
    return toJson(src, false);
  }

  public static String toJson(final Object src, final boolean prettyPrint) {
    try {
      if (prettyPrint) {
        return DataFormatUtil.json().asPrettyPrintString(src);
      }
      return DataFormatUtil.json().asString(src);
    } catch (final IOException e) {
      throw new UnsupportedOperationException(e);
    }
  }

  public static JsonElement toJsonTree(final Object src) {
    try {
      return DataFormatUtil.json().fromTreeNode(toTreeNode(src), JsonElement.class);
    } catch (final IOException e) {
      Logger.debug(e, "unable to convert object {}", src);
      throw new UnsupportedOperationException(e);
    }
  }

  public static JsonNode toTreeNode(final Object src) {
    return DataFormatUtil.json().toTreeNode(src);
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



  public static class Test {
    final HashIndexedArray<String> h = new HashIndexedArray<>(new String[] { "a", "b", "c" });
    final HashIndexedArrayMap<String, Long[]> h2 = new HashIndexedArrayMap<>(new String[] { "aa", "bb" }, new Long[][] {
      new Long[] { 1L, 2L }, new Long[] { 3L }
    });

    public String toString() {
      return h.toString() + " - " + h2.toString();
    }
  }

  public interface Jsonable {
    JsonElement toJson();
  }

  public static void main(final String[] args) throws Exception {
    final Test t = new Test();
    final String json = JsonUtil.toJson(t);
    System.out.println(json);
    System.out.println(JsonUtil.fromJson(json, Test.class));
  }
}
