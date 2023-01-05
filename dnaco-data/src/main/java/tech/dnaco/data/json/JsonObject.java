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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JsonObject extends JsonElement {
  private final Map<String, JsonElement> members;

  public JsonObject() {
    this.members = new HashMap<>();
  }

  public JsonObject(final int length) {
    this.members = new HashMap<>(length);
  }

  public JsonObject(final JsonObject other) {
    this.members = new HashMap<>(other.members);
  }

  public JsonObject(final String key, final JsonElement value) {
    this.members = Map.of(key, value);
  }

  public boolean isEmpty() {
    return members.isEmpty();
  }

  public boolean isNotEmpty() {
    return !members.isEmpty();
  }

  public int size() {
    return members.size();
  }

  protected Map<String, JsonElement> getMembers() {
    return members;
  }

  public Set<String> keySet() {
    return members.keySet();
  }

  public Set<Map.Entry<String, JsonElement>> entrySet() {
    return members.entrySet();
  }

  public boolean has(final String memberName) {
    return members.containsKey(memberName);
  }

  public boolean hasObject(final String memberName) {
    final JsonElement element = members.get(memberName);
    return element != null && element.isJsonObject();
  }

  public boolean hasArray(final String memberName) {
    final JsonElement element = members.get(memberName);
    return element != null && element.isJsonArray();
  }

  public JsonElement get(final String memberName) {
    return members.get(memberName);
  }

  public JsonObject addNull(final String property) {
    members.put(property, JsonNull.INSTANCE);
    return this;
  }

  public JsonObject addAll(final JsonObject other) {
    members.putAll(other.members);
    return this;
  }

  public JsonObject add(final String property, final JsonElement value) {
    members.put(property, value == null ? JsonNull.INSTANCE : value);
    return this;
  }

  public JsonObject add(final String property, final boolean value) {
    return add(property, new JsonPrimitive(value));
  }

  public JsonObject add(final String property, final long value) {
    return add(property, new JsonPrimitive(value));
  }

  public JsonObject add(final String property, final double value) {
    return add(property, new JsonPrimitive(value));
  }

  public JsonObject add(final String property, final String value) {
    return add(property, value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
  }

  public JsonObject add(final String property, final Number value) {
    return add(property, value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
  }

  public JsonObject addProperty(final String property, final boolean value) {
    return add(property, value);
  }

  public JsonObject addProperty(final String property, final long value) {
    return add(property, value);
  }

  public JsonObject addProperty(final String property, final double value) {
    return add(property, value);
  }

  public JsonObject addProperty(final String property, final String value) {
    return add(property, value);
  }

  public JsonObject addProperty(final String property, final Number value) {
    return add(property, value);
  }

  public void clear() {
    members.clear();
  }

  public JsonElement remove(final String key) {
    return members.remove(key);
  }

  public JsonObject getAsJsonObject(final String memberName) {
    return (JsonObject) members.get(memberName);
  }

  @Override
  public int hashCode() {
    return members.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    return (o == this) || (o instanceof JsonObject && ((JsonObject) o).members.equals(members));
  }
}
