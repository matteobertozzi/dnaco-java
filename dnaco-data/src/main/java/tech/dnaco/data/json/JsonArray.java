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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JsonArray extends JsonElement implements Iterable<JsonElement> {
  private final ArrayList<JsonElement> elements;

  public JsonArray() {
    elements = new ArrayList<>();
  }

  public JsonArray(final int capacity) {
    elements = new ArrayList<>(capacity);
  }

  public int size() {
    return elements.size();
  }

  public boolean isEmpty() {
    return elements.isEmpty();
  }

  public boolean isNotEmpty() {
    return !elements.isEmpty();
  }

  protected List<JsonElement> getElements() {
    return elements;
  }

  public JsonElement get(final int index) {
    return elements.get(index);
  }

  public JsonArray add(final boolean value) {
    return add(new JsonPrimitive(value));
  }

  public JsonArray add(final long value) {
    return add(new JsonPrimitive(value));
  }

  public JsonArray add(final double value) {
    return add(new JsonPrimitive(value));
  }

  public JsonArray add(final String value) {
    return add(value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
  }

  public JsonArray add(final JsonElement element) {
    elements.add(element != null ? element : JsonNull.INSTANCE);
    return this;
  }

  public JsonArray set(final int index, final JsonElement element) {
    elements.set(index, element);
    return this;
  }

  public JsonArray remove(final int index) {
    elements.remove(index);
    return this;
  }

  @Override
  public Iterator<JsonElement> iterator() {
    return elements.iterator();
  }

  @Override
  public int hashCode() {
    return elements.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    return (o == this) || (o instanceof JsonArray && ((JsonArray) o).elements.equals(elements));
  }
}
