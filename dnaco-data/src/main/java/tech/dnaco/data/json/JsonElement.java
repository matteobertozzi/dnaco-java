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

import com.fasterxml.jackson.databind.JsonNode;

import tech.dnaco.data.JsonFormat;

public abstract class JsonElement {
  public boolean isJsonNull() {
    return this instanceof JsonNull;
  }

  public boolean isJsonPrimitive() {
    return this instanceof JsonPrimitive;
  }

  public boolean isJsonArray() {
    return this instanceof JsonArray;
  }

  public boolean isJsonObject() {
    return this instanceof JsonObject;
  }

  public JsonPrimitive getAsJsonPrimitive() {
    if (isJsonPrimitive()) {
      return (JsonPrimitive) this;
    }
    throw new IllegalStateException("Not a JSON Primitive: " + this);
  }

  public JsonObject getAsJsonObject() {
    if (isJsonObject()) {
      return (JsonObject) this;
    }
    throw new IllegalStateException("Not a JSON Object: " + this);
  }

  public JsonArray getAsJsonArray() {
    if (isJsonArray()) {
      return (JsonArray) this;
    }
    throw new IllegalStateException("Not a JSON Array: " + this);
  }

  public boolean getAsBoolean() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  public Number getAsNumber() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  public String getAsString() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  public double getAsDouble() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  public float getAsFloat() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  public long getAsLong() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  public int getAsInt() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  public byte getAsByte() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  public short getAsShort() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  public JsonNode toTreeNode() {
    return JsonFormat.INSTANCE.toTreeNode(this);
  }

  @Override
  public String toString() {
    return JsonFormat.INSTANCE.asString(this);
  }
}
