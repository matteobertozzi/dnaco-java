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

package tech.dnaco.dispatcher;

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

import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.arrays.ByteArray;
import tech.dnaco.data.hashes.Hash;
import tech.dnaco.data.modules.DataMapperModules;
import tech.dnaco.strings.BaseX;
import tech.dnaco.util.RandData;

public class ExecutionId implements Comparable<ExecutionId> {
  static {
    final SimpleModule dataMapperModule = new SimpleModule();
    dataMapperModule.addSerializer(ExecutionId.class, new ExecutionIdSerializer());
    dataMapperModule.addDeserializer(ExecutionId.class, new ExecutionIdDeserializer());
    DataMapperModules.INSTANCE.registerModule(dataMapperModule);
  }

  private static final long TIMESTAMP_OFFSET = 1640991600L;

  private final long ts;
  private final long hi;
  private final long lo;

  private ExecutionId(final long ts, final long lo, final long hi) {
    this.ts = ts;
    this.lo = lo;
    this.hi = hi;
  }

  private ExecutionId(final long timestamp, final int format, final int shard, final long lo, final long hi) {
    this(((timestamp - TIMESTAMP_OFFSET) << 23) | (format & 0x7f) << 16 | (shard & 0xffff), lo, hi);
  }

  public static ExecutionId randomId() {
    final byte[] randomBytes = new byte[18];
    RandData.generateBytes(randomBytes);

    final long lo = IntDecoder.BIG_ENDIAN.readFixed64(randomBytes, 0);
    final long hi = IntDecoder.BIG_ENDIAN.readFixed64(randomBytes, 8);
    final int shard = IntDecoder.BIG_ENDIAN.readFixed16(randomBytes, 16);
    return new ExecutionId(System.currentTimeMillis(), 0, shard, lo, hi);
  }

  public static ExecutionId randomId(final int shard) {
    final byte[] randomBytes = new byte[16];
    RandData.generateBytes(randomBytes);

    final long lo = IntDecoder.BIG_ENDIAN.readFixed64(randomBytes, 0);
    final long hi = IntDecoder.BIG_ENDIAN.readFixed64(randomBytes, 8);
    return new ExecutionId(System.currentTimeMillis(), 0, shard, lo, hi);
  }

  public void addToHash(final Hash hash) {
    hash.update(ts);
    hash.update(lo);
    hash.update(hi);
  }

  public byte[] toBytes() {
    final byte[] buffer = new byte[24];
    IntEncoder.BIG_ENDIAN.writeFixed64(buffer,  0, ts);
    IntEncoder.BIG_ENDIAN.writeFixed64(buffer,  8, lo);
    IntEncoder.BIG_ENDIAN.writeFixed64(buffer, 16, hi);
    return buffer;
  }

  public void writeTo(final ByteArray buffer) {
    IntEncoder.BIG_ENDIAN.writeFixed(buffer, ts, 8);
    IntEncoder.BIG_ENDIAN.writeFixed(buffer, lo, 8);
    IntEncoder.BIG_ENDIAN.writeFixed(buffer, hi, 8);
  }

  @Override
  public String toString() {
    return BaseX.encode64(toBytes());
  }

  public static ExecutionId fromString(final String id) {
    return fromBytes(BaseX.decode64(id));
  }

  public static ExecutionId fromBytes(final byte[] id) {
    return fromBytes(id, 0);
  }

  public static ExecutionId fromBytes(final byte[] id, final int off) {
    final long ts = IntDecoder.BIG_ENDIAN.readFixed64(id, off);
    final long lo = IntDecoder.BIG_ENDIAN.readFixed64(id, off + 8);
    final long hi = IntDecoder.BIG_ENDIAN.readFixed64(id, off + 16);
    return new ExecutionId(ts, lo, hi);
  }

  public long timestamp() {
    return TIMESTAMP_OFFSET + (ts >>> 23);
  }

  public int shard() {
    return (int) (ts & 0xffff);
  }

  @Override
  public int hashCode() {
    int result;
    result = 31 + (int) (ts ^ (ts >>> 32));
    result = 31 * result + (int) (lo ^ (lo >>> 32));
    result = 31 * result + (int) (hi ^ (hi >>> 32));
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (!(obj instanceof final ExecutionId other)) return false;
    return (hi == other.hi) && (lo == other.lo) && (ts == other.ts);
  }

  @Override
  public int compareTo(final ExecutionId other) {
    int cmp = Long.compare(ts, other.ts);
    if (cmp != 0) return cmp;
    cmp = Long.compare(lo, other.lo);
    if (cmp != 0) return cmp;
    return Long.compare(hi, other.hi);
  }

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
}
