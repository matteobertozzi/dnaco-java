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
import java.lang.reflect.Type;
import java.util.Base64;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.ArrayUtil;

public final class JsonUtil {
  public static final String JSON_DATE_FORMAT_PATTERN = "YYYYMMddHHmmss";
  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(byte[].class, new BytesTypeAdapter())
      .setDateFormat(JSON_DATE_FORMAT_PATTERN)
      .create();

  private JsonUtil() {
    // no-op
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
  //  Json Array helpers
  // ================================================================================
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
}
