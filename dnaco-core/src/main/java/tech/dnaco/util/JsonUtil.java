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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import tech.dnaco.collections.ArrayUtil;

public final class JsonUtil {
  private static final Gson GSON = new Gson();

  private JsonUtil() {
    // no-op
  }

  // ================================================================================
  //  From Json helpers
  // ================================================================================
  public static <T> T fromJson(final JsonElement json, final Class<T> classOfT) {
    return GSON.fromJson(json, classOfT);
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
}
