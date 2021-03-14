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
import io.netty.buffer.Unpooled;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;

public class DnacoRpcRequest extends DnacoRpcPacket {
  public static final AtomicLong ALLOCATED = new AtomicLong();

  public enum SendResultTo { CALLER, STORE_IN_MEMORY, STORE_WITH_ID, FORWARD_TO }

  private static final SendResultTo[] SEND_RESULT_TO = SendResultTo.values();

  private SendResultTo sendResultTo;
  private ByteBuf requestId;
  private ByteBuf resultId;

  private DnacoRpcRequest() {
    // no-op
  }

  @Override
  protected PacketType getPacketType() {
    return PacketType.REQUEST;
  }

  public static DnacoRpcRequest alloc(final TraceId traceId, final SpanId spanId, final long pkgId,
      final ByteBuf requestId, final ByteBuf data,
      final SendResultTo sendResultTo, final ByteBuf resultId)
  {
    final DnacoRpcRequest request = new DnacoRpcRequest();
    request.setPacket(traceId, spanId, pkgId, data);
    request.requestId = requestId;
    request.sendResultTo = sendResultTo;
    request.resultId = resultId != null ? resultId : Unpooled.EMPTY_BUFFER;
    //System.out.println("ALLOC RPC REQ " + request.getPacketId());
    ALLOCATED.incrementAndGet();
    return request;
  }

  protected static DnacoRpcRequest alloc(final TraceId traceId, final SpanId spanId, final long pkgId,
      final ByteBuf requestId, final ByteBuf data,
      final int sendResultTo, final ByteBuf resultId)
  {
    return alloc(traceId, spanId, pkgId, requestId, data, SEND_RESULT_TO[sendResultTo], resultId);
  }

  @Override
  public void deallocate() {
    this.requestId.release();
    this.resultId.release();
    ALLOCATED.decrementAndGet();
    super.deallocate();
  }

  public ByteBuf getRequestId() {
    return requestId;
  }

  public SendResultTo getSendResultTo() {
    return sendResultTo;
  }

  public boolean hasResultId() {
    return sendResultTo != SendResultTo.CALLER;
  }

  public ByteBuf getResultId() {
    return resultId;
  }

  @Override
  public String toString() {
    return "DnacoRpcRequest [traceId=" + getTraceId() + ", spanId=" + getSpanId() + ", packetId=" + getPacketId()
      + ", requestId=" + requestId.toString(StandardCharsets.UTF_8)
      + ", sendResultTo=" + sendResultTo
      + ", resultId=" + resultId.toString(StandardCharsets.UTF_8)
      + ", data=" + getDataSize()
      + "]";
  }
}
