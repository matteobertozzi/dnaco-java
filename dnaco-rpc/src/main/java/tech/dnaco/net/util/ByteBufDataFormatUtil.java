/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.net.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import tech.dnaco.data.DataFormat;
import tech.dnaco.data.modules.DataMapperModules;

public final class ByteBufDataFormatUtil {
  private ByteBufDataFormatUtil() {
    // no-op
  }

  public static <T> T fromBytes(final DataFormat format, final ByteBuf data, final Class<T> valueType) throws IOException {
    final InputStream stream = new ByteBufInputStream(data);
    try {
      return format.fromStream(stream, valueType);
    } finally {
      stream.reset();
    }
  }

  public static void addToBytes(final DataFormat format, final ByteBuf buffer, final Object obj) throws IOException {
    final OutputStream stream = new ByteBufOutputStream(buffer);
    format.addToStream(stream, obj);
    stream.flush();
  }

  private static final SimpleModule BYTE_BUF_MODULE = new SimpleModule();
  static {
    BYTE_BUF_MODULE.addSerializer(ByteBuf.class, new ByteBufJsonSerializer());
    BYTE_BUF_MODULE.addDeserializer(ByteBuf.class, new ByteBufJsonDeserializer());

    DataMapperModules.INSTANCE.registerModule(BYTE_BUF_MODULE);
  }

  public static final class ByteBufJsonSerializer extends StdSerializer<ByteBuf> {
	  private static final long serialVersionUID = -2793630286053545729L;

	  public ByteBufJsonSerializer() {
      super(ByteBuf.class);
    }

    @Override
    public void serialize(final ByteBuf value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
      final ByteBufInputStream stream = new ByteBufInputStream(value);
      try {
        gen.writeBinary(stream, value.readableBytes());
      } finally {
        stream.reset();
      }
    }
  }

  public static final class ByteBufJsonDeserializer extends StdDeserializer<ByteBuf> {
	  private static final long serialVersionUID = 1283404414554988558L;

    public ByteBufJsonDeserializer() {
      super(ByteBuf.class);
    }

	  @Override
    public ByteBuf deserialize(final JsonParser parser, final DeserializationContext ctx) throws IOException {
      return Unpooled.wrappedBuffer(parser.getBinaryValue());
    }
  }
}
