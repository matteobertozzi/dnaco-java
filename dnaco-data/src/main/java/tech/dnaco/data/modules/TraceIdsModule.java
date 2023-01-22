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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;
import tech.dnaco.xtracing.ExecutionId;

public final class TraceIdsModule {
  public static final SimpleModule INSTANCE = new SimpleModule();
  static {
    INSTANCE.addSerializer(TraceId.class, new TraceIdSerializer());
    INSTANCE.addDeserializer(TraceId.class, new TraceIdDeserializer());

    INSTANCE.addSerializer(SpanId.class, new SpanIdSerializer());
    INSTANCE.addDeserializer(SpanId.class, new SpanIdDeserializer());

    INSTANCE.addSerializer(ExecutionId.class, new ExecutionIdSerializer());
    INSTANCE.addDeserializer(ExecutionId.class, new ExecutionIdDeserializer());
  }

  private TraceIdsModule() {
    // no-op
  }

  // ====================================================================================================
  //  TraceId related
  // ====================================================================================================
  public static final class TraceIdSerializer extends StdSerializer<TraceId> {
	  private static final long serialVersionUID = -6449214149339710750L;

	  public TraceIdSerializer() {
      super(TraceId.class);
    }

    @Override
    public void serialize(final TraceId value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
      gen.writeString(value.toString());
    }
  }

  private static final class TraceIdDeserializer extends StdDeserializer<TraceId> {
    private TraceIdDeserializer() {
      super(TraceId.class);
    }

    @Override
    public TraceId deserialize(final JsonParser parser, final DeserializationContext ctxt) throws IOException, JacksonException {
      final JsonNode node = parser.getCodec().readTree(parser);
      if (node.isBinary()) {
        return TraceId.fromBytes(node.binaryValue());
      } else if (node.isTextual()) {
        return TraceId.fromString(node.textValue());
      } else {
        throw new IllegalArgumentException();
      }
    }
  }

  // ====================================================================================================
  //  SpanId related
  // ====================================================================================================
  public static final class SpanIdSerializer extends StdSerializer<SpanId> {
	  private static final long serialVersionUID = -6449214149339710750L;

	  public SpanIdSerializer() {
      super(SpanId.class);
    }

    @Override
    public void serialize(final SpanId value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
      gen.writeString(value.toString());
    }
  }

  private static final class SpanIdDeserializer extends StdDeserializer<SpanId> {
    private SpanIdDeserializer() {
      super(SpanId.class);
    }

    @Override
    public SpanId deserialize(final JsonParser parser, final DeserializationContext ctxt) throws IOException, JacksonException {
      final JsonNode node = parser.getCodec().readTree(parser);
      if (node.isBinary()) {
        return SpanId.fromBytes(node.binaryValue());
      } else if (node.isTextual()) {
        return SpanId.fromString(node.textValue());
      } else {
        throw new IllegalArgumentException();
      }
    }
  }

  // ====================================================================================================
  //  ExecutionId related
  // ====================================================================================================
  private static final class ExecutionIdSerializer extends StdSerializer<ExecutionId> {
    private ExecutionIdSerializer() {
      super(ExecutionId.class);
    }

    @Override
    public void serialize(final ExecutionId value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
      if (gen.canWriteBinaryNatively()) {
        gen.writeBinary(value.toBytes());
      } else {
        gen.writeString(value.toString());
      }
    }
  }

  private static final class ExecutionIdDeserializer extends StdDeserializer<ExecutionId> {
    private ExecutionIdDeserializer() {
      super(ExecutionId.class);
    }

    @Override
    public ExecutionId deserialize(final JsonParser parser, final DeserializationContext ctxt) throws IOException, JacksonException {
      final JsonNode node = parser.getCodec().readTree(parser);
      if (node.isBinary()) {
        return ExecutionId.fromBytes(node.binaryValue());
      } else if (node.isTextual()) {
        return ExecutionId.fromString(node.textValue());
      } else {
        throw new IllegalArgumentException();
      }
    }
  }

  public static void main(final String[] args) {
    final String x = JsonUtil.toJson(TraceId.newRandomId());
    final TraceId y = JsonUtil.fromJson(x, TraceId.class);
    System.out.println(x + " " + y);
  }
}
