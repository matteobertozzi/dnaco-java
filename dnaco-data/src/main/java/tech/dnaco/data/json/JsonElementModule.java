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

import java.io.IOException;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public final class JsonElementModule {
  public static final SimpleModule INSTANCE = new SimpleModule();
  static {
    INSTANCE.addSerializer(JsonElement.class, new JsonElementSerializer());
    INSTANCE.addDeserializer(JsonElement.class, new JsonElementDeserializer());
    INSTANCE.addDeserializer(JsonObject.class, new JsonObjectDeserializer());
    INSTANCE.addDeserializer(JsonArray.class, new JsonArrayDeserializer());
    INSTANCE.addDeserializer(JsonPrimitive.class, new JsonPrimitiveDeserializer());
  }

  public static final class JsonElementSerializer extends StdSerializer<JsonElement> {
	  private static final long serialVersionUID = -6449214149339710750L;

	  public JsonElementSerializer() {
      super(JsonElement.class);
    }

    @Override
    public void serialize(final JsonElement value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
      if (value.isJsonObject()) {
        provider.defaultSerializeValue(value.getAsJsonObject().getMembers(), gen);
      } else if (value.isJsonArray()) {
        provider.defaultSerializeValue(value.getAsJsonArray().getElements(), gen);
      } else if (value.isJsonPrimitive()) {
        provider.defaultSerializeValue(value.getAsJsonPrimitive().getValue(), gen);
      } else if (value.isJsonNull()) {
        gen.writeNull();
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  public static final class JsonObjectDeserializer extends StdDeserializer<JsonObject> {
    private static final long serialVersionUID = 1L;

    public JsonObjectDeserializer() {
      super(JsonObject.class);
    }

	  @Override
    public JsonObject deserialize(final JsonParser parser, final DeserializationContext ctx) throws IOException {
      final JsonNode node = parser.getCodec().readTree(parser);
      return (JsonObject) fromJsonNode(node);
    }
  }

  public static final class JsonArrayDeserializer extends StdDeserializer<JsonArray> {
    private static final long serialVersionUID = 1L;

    public JsonArrayDeserializer() {
      super(JsonArray.class);
    }

	  @Override
    public JsonArray deserialize(final JsonParser parser, final DeserializationContext ctx) throws IOException {
      final JsonNode node = parser.getCodec().readTree(parser);
      return (JsonArray) fromJsonNode(node);
    }
  }

  public static final class JsonPrimitiveDeserializer extends StdDeserializer<JsonPrimitive> {
    private static final long serialVersionUID = 1L;

    public JsonPrimitiveDeserializer() {
      super(JsonPrimitive.class);
    }

	  @Override
    public JsonPrimitive deserialize(final JsonParser parser, final DeserializationContext ctx) throws IOException {
      final JsonNode node = parser.getCodec().readTree(parser);
      return (JsonPrimitive) fromJsonNode(node);
    }
  }

  public static final class JsonElementDeserializer extends StdDeserializer<JsonElement> {
    private static final long serialVersionUID = 7601352803339381468L;

    public JsonElementDeserializer() {
      super(JsonElement.class);
    }

	  @Override
    public JsonElement deserialize(final JsonParser parser, final DeserializationContext ctx) throws IOException {
      final JsonNode node = parser.getCodec().readTree(parser);
      return fromJsonNode(node);
    }
  }

  private static JsonElement fromJsonNode(final JsonNode node) throws IOException {
    if (node.isNull()) return JsonNull.INSTANCE;

    if (node.isObject()) {
      final JsonObject jsonObject = new JsonObject(node.size());
      final Iterator<Entry<String, JsonNode>> it = node.fields();
      while (it.hasNext()) {
        final Entry<String, JsonNode> entry = it.next();
        jsonObject.add(entry.getKey(), fromJsonNode(entry.getValue()));
      }
      return jsonObject;
    }

    if (node.isArray()) {
      final JsonArray jsonArray = new JsonArray(node.size());
      for (final JsonNode item: node) {
        jsonArray.add(fromJsonNode(item));
      }
      return jsonArray;
    }

    if (node.isValueNode()) {
      if (node.isTextual()) return new JsonPrimitive(node.textValue());
      if (node.isFloat()) return new JsonPrimitive(node.floatValue());
      if (node.isDouble()) return new JsonPrimitive(node.doubleValue());
      if (node.isLong()) return new JsonPrimitive(node.longValue());
      if (node.isInt()) return new JsonPrimitive(node.intValue());
      if (node.isBoolean()) return new JsonPrimitive(node.booleanValue());
      if (node.isBinary()) return new JsonPrimitive(Base64.getEncoder().encodeToString(node.binaryValue()));
    }

    throw new IOException(node.getNodeType() + ": " + node);
  }
}
