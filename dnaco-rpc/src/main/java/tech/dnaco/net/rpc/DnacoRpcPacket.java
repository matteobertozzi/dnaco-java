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

import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;

public abstract class DnacoRpcPacket extends AbstractReferenceCounted {
  protected enum PacketType { REQUEST, RESPONSE, EVENT, CONTROL }

  private long stampNs;
  private TraceId traceId;
  private SpanId spanId;
  private long packetId;
  private ByteBuf data;

  protected abstract PacketType getPacketType();

  @Override
  protected void deallocate() {
    this.data.release();
  }

  @Override
  public ReferenceCounted touch(final Object hint) {
    return this;
  }

  protected void setPacket(final TraceId traceId, final SpanId spanId, final long packetId, final ByteBuf data) {
    Objects.requireNonNull(traceId);
    Objects.requireNonNull(spanId);

    if (this.refCnt() == 0) this.setRefCnt(1);
    this.stampNs = System.nanoTime();
    this.packetId = packetId;
    this.traceId = traceId;
    this.spanId = spanId;
    this.data = data != null ? data : Unpooled.EMPTY_BUFFER;
  }

  public TraceId getTraceId() {
    return traceId;
  }

  public SpanId getSpanId() {
    return spanId;
  }

  public long getPacketId() {
    return packetId;
  }

  public long getStampNs() {
    return stampNs;
  }

  public int getRev() {
    return 0;
  }

  public ByteBuf getData() {
    return data;
  }

  public int getDataSize() {
    return data.readableBytes();
  }
}
