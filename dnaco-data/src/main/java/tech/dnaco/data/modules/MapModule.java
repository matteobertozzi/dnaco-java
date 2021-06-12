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

package tech.dnaco.data.modules;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import tech.dnaco.collections.maps.HashIndexedArrayMap;
import tech.dnaco.collections.sets.HashIndexedArray;
import tech.dnaco.collections.sets.IndexedHashSet;
import tech.dnaco.data.JsonFormat;

public final class MapModule {
  public static final SimpleModule INSTANCE = new SimpleModule();
  static {
    INSTANCE.addSerializer(new HashIndexedArraySerializer());
    INSTANCE.addDeserializer(HashIndexedArray.class, new HashIndexedArrayDeserializer());
    INSTANCE.addSerializer(new HashIndexedArrayMapSerializer());
    INSTANCE.addDeserializer(HashIndexedArrayMap.class, new HashIndexedArrayMapDeserializer());
    INSTANCE.addSerializer(new IndexedHashSetSerializer());
  }

  private MapModule() {
    // no-op
  }

  public static final class IndexedHashSetSerializer extends StdSerializer<IndexedHashSet<?>> {
	  private static final long serialVersionUID = -6449214149339710750L;

	  public IndexedHashSetSerializer() {
      super(IndexedHashSet.class, false);
    }

    @Override
    public void serialize(final IndexedHashSet<?> value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
      provider.defaultSerializeValue(value.keys(), gen);
    }
  }

  public static final class HashIndexedArraySerializer extends StdSerializer<HashIndexedArray<?>> {
	  private static final long serialVersionUID = -6449214149339710750L;

	  public HashIndexedArraySerializer() {
      super(HashIndexedArray.class, false);
    }

    @Override
    public void serialize(final HashIndexedArray<?> value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
      provider.defaultSerializeValue(value.keySet(), gen);
    }
  }

  public static final class HashIndexedArrayDeserializer extends StdDeserializer<HashIndexedArray<?>> {
    private static final long serialVersionUID = 7601352803339381468L;

    public HashIndexedArrayDeserializer() {
      super(HashIndexedArray.class);
    }

	  @Override
    public HashIndexedArray<?> deserialize(final JsonParser parser, final DeserializationContext ctx) throws IOException {
      final JsonNode node = parser.getCodec().readTree(parser);
      if (!node.isArray()) throw new JsonParseException(parser, "expected array got " + node.getNodeType());

      final JsonNode firstNode = node.get(0);
      if (firstNode.isTextual()) {
        final String[] keys = new String[node.size()];
        for (int i = 0; i < keys.length; ++i) {
          keys[i] = node.get(i).asText();
        }
        return new HashIndexedArray<>(keys);
      }
      throw new JsonParseException(parser, "unhandled type: " + firstNode.getNodeType());
    }
  }

  public static final class HashIndexedArrayMapSerializer extends StdSerializer<HashIndexedArrayMap<?, ?>> {
	  private static final long serialVersionUID = -6449214149339710750L;

	  public HashIndexedArrayMapSerializer() {
      super(HashIndexedArrayMap.class, false);
    }

    @Override
    public void serialize(final HashIndexedArrayMap<?, ?> value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
      gen.writeStartObject();
      for (int i = 0, n = value.size(); i < n; ++i) {
        gen.writeFieldName(String.valueOf(value.getKey(i)));
        provider.defaultSerializeValue(value.get(i), gen);
      }
      gen.writeEndObject();
    }
  }

  public static final class HashIndexedArrayMapDeserializer extends StdDeserializer<HashIndexedArrayMap<?, ?>> {
    private static final long serialVersionUID = 7601352803339381468L;

    public HashIndexedArrayMapDeserializer() {
      super(HashIndexedArrayMap.class);
    }

	  @Override
    public HashIndexedArrayMap<?, ?> deserialize(final JsonParser parser, final DeserializationContext ctx) throws IOException {
      final JsonNode node = parser.getCodec().readTree(parser);
      if (!node.isObject()) throw new IOException("expected object got " + node.getNodeType());

      final Object[] keys = new Object[node.size()];
      final Object[] vals = new Object[keys.length];
      int index = 0;
      final Iterator<Entry<String, JsonNode>> it = node.fields();
      while (it.hasNext()) {
        final Entry<String, JsonNode> entry = it.next();
        keys[index] = entry.getKey();
        vals[index] = JsonFormat.INSTANCE.fromTreeNode(entry.getValue(), Object.class);
        index++;
      }
      return new HashIndexedArrayMap<>(keys, vals);
    }
  }
}
