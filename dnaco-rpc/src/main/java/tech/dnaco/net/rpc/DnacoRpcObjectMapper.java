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

package tech.dnaco.net.rpc;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import tech.dnaco.data.CborFormat;
import tech.dnaco.data.JsonFormat;
import tech.dnaco.net.util.ByteBufDataFormatUtil;

public interface DnacoRpcObjectMapper {
  <T> T fromBytes(ByteBuf data, Class<T> type) throws IOException;
  ByteBuf toBytes(Object data, Class<?> type) throws IOException;

  default ByteBuf toBytes(final Object data) throws IOException {
    return data != null ? toBytes(data, data.getClass()) : null;
  }

  DnacoRpcObjectMapper RPC_CBOR_OBJECT_MAPPER = new DnacoRpcObjectMapper() {
    @Override
    public <T> T fromBytes(final ByteBuf data, final Class<T> type) throws IOException {
      return ByteBufDataFormatUtil.fromBytes(CborFormat.INSTANCE, data, type);
    }

    @Override
    public ByteBuf toBytes(final Object result, final Class<?> type) throws IOException {
      final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
      try {
        ByteBufDataFormatUtil.addToBytes(CborFormat.INSTANCE, buffer, result);
        return buffer;
      } catch (final Throwable e) {
        buffer.release();
        throw e;
      }
    }
  };

  DnacoRpcObjectMapper RPC_JSON_OBJECT_MAPPER = new DnacoRpcObjectMapper() {
    @Override
    public <T> T fromBytes(final ByteBuf data, final Class<T> type) throws IOException {
      return ByteBufDataFormatUtil.fromBytes(JsonFormat.INSTANCE, data, type);
    }

    @Override
    public ByteBuf toBytes(final Object result, final Class<?> type) throws IOException {
      final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
      try {
        ByteBufDataFormatUtil.addToBytes(JsonFormat.INSTANCE, buffer, result);
        return buffer;
      } catch (final Throwable e) {
        buffer.release();
        throw e;
      }
    }
  };
}
