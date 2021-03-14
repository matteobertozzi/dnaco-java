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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;

public class DnacoRpcEvent extends DnacoRpcPacket {
  public static final AtomicLong ALLOCATED = new AtomicLong();

  private enum Flags { NONE, REQUIRE_ACK }

  private ByteBuf eventId;

  private DnacoRpcEvent() {
    // no-op
  }

	@Override
	protected PacketType getPacketType() {
		return PacketType.EVENT;
	}

	public static DnacoRpcEvent alloc(final TraceId traceId, final SpanId spanId, 
      final long pkgId, final int pkgFlags,
      final ByteBuf eventId, final ByteBuf data) {
    final DnacoRpcEvent event = new DnacoRpcEvent();
		event.setPacket(traceId, spanId, pkgId, data);
    // TODO: handle pkgFlags
    event.eventId = eventId;
    ALLOCATED.incrementAndGet();
    return event;
  }

	@Override
	protected void deallocate() {
    this.eventId.release();
    ALLOCATED.decrementAndGet();
    super.deallocate();
	}

	public ByteBuf getEventId() {
		return eventId;
  }

  @Override
  public String toString() {
    return "DnacoRpcEvent [traceId=" + getTraceId() + ", spanId=" + getSpanId() + ", packetId=" + getPacketId()
      + ", eventId=" + eventId.toString(StandardCharsets.UTF_8)
      + ", data=" + getDataSize()
      + "]";
  }
}
