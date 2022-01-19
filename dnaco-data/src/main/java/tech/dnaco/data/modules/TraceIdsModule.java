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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;

public final class TraceIdsModule {
  public static final SimpleModule INSTANCE = new SimpleModule();
  static {
    INSTANCE.addSerializer(TraceId.class, new TraceIdSerializer());
    INSTANCE.addSerializer(SpanId.class, new SpanIdSerializer());
  }

  private TraceIdsModule() {
    // no-op
  }

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

  public static void main(final String[] args) {
    final String x = JsonUtil.toJson(TraceId.newRandomId());
    final TraceId y = JsonUtil.fromJson(x, TraceId.class);
    System.out.println(x + " " + y);
  }
}
