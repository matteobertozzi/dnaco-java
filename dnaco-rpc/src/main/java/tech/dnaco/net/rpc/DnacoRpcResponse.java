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

import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;

public class DnacoRpcResponse extends DnacoRpcPacket {
  public static final AtomicLong ALLOCATED = new AtomicLong();

  public enum OperationStatus { SUCCEEDED, FAILED, CANCELLED }
  private static final OperationStatus[] OPERATION_STATUS = OperationStatus.values();

  private OperationStatus status;
  private long queueTime;
  private long execTime;

  @Override
  protected PacketType getPacketType() {
    return PacketType.RESPONSE;
  }

  public static DnacoRpcResponse alloc(final TraceId traceId, final SpanId spanId, final long pkgId,
    final OperationStatus status, final long queueTime, final long execTime, final ByteBuf data) {
    final DnacoRpcResponse response = new DnacoRpcResponse();
    response.setPacket(traceId, spanId, pkgId, data);
    response.status = status;
    response.queueTime = queueTime;
    response.execTime = execTime;
    //System.out.println("ALLOC RPC RESP " + pkgId);
    ALLOCATED.incrementAndGet();
    return response;
  }

  protected static DnacoRpcResponse alloc(final TraceId traceId, final SpanId spanId, final long pkgId,
    final int opStatus, final long queueTime, final long execTime, final ByteBuf data) {
    return alloc(traceId, spanId, pkgId, OPERATION_STATUS[opStatus], queueTime, execTime, data);
  }

  @Override
  protected void deallocate() {
    ALLOCATED.decrementAndGet();
    super.deallocate();
    //System.out.println("DEALLOC RPC RESP " + this.getPacketId());
  }

  public OperationStatus getStatus() {
    return status;
  }

  public long getQueueTime() {
    return queueTime;
  }

  public long getExecTime() {
    return execTime;
  }

  public OperationStatus getOperationStatus() {
    return status;
  }

  @Override
  public String toString() {
    return "DnacoRpcResponse [traceId=" + getTraceId() + ", packetId=" + getPacketId()
      + ", queueTime=" + HumansUtil.humanTimeNanos(queueTime)
      + ", execTime=" + HumansUtil.humanTimeNanos(execTime)
      + ", status=" + status
      + ", data=" + getDataSize()
      + "]";
  }
}
