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
import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.frame.DnacoFrame;
import tech.dnaco.net.frame.DnacoFrameUtil;
import tech.dnaco.tracing.SpanId;
import tech.dnaco.tracing.TraceId;

public final class DnacoRpcPacketUtil {
  private DnacoRpcPacketUtil() {
    // no-op
  }

  public static void decode(final ByteBuf in, final Consumer<DnacoRpcPacket> consumer) {
    DnacoFrameUtil.decodeFrames(in, Integer.MAX_VALUE, (frame) -> {
      final DnacoRpcPacket packet;
      try {
        packet = DnacoRpcPacketUtil.decodeRpc(frame);
      } finally {
        frame.release();
      }

      try {
        consumer.accept(packet);
      } finally {
        packet.release();
      }
    });
  }

  public static DnacoRpcPacket decodeRpc(final DnacoFrame frame) {
    // RPC Header packets are composed of:
    // - Packet Type: 2bit (REQUEST, RESPONSE, EVENT, CONTROL)
    // - Flags: 3bit (specific to packet type)
    // - Packet Id length: 3bit (1 + (0-7)) max 8bytes int
    //   +----+-----+-----+ +----------+ +---------+ +-----------+
    //   | 11 | --- | 111 | | Trace Id | | Span Id | | Packet Id |
    //   +----+-----+-----+ +----------+ +---------+ +-----------+
    //   0    2     5     8          136         200  (1-8 bytes)
    final ByteBuf frameData = frame.getData();
    final int rpcHead = frameData.readByte() & 0xff;
    final int pkgType = (rpcHead >> 6) & 0x3;
    final int pkgFlags = (rpcHead >> 3) & 0x3;
    final int pkgIdLen = 1 + (rpcHead & 0x7);

    final long traceIdHi = frameData.readLong();
    final long traceIdLo = frameData.readLong();
    final TraceId traceId = new TraceId(traceIdHi, traceIdLo);
    final SpanId spanId = new SpanId(frameData.readLong());
    final long pkgId = readLong(frameData, pkgIdLen);

    //System.out.println("DECODE RPC " + traceId + " " + pkgId + " " + pkgType);
    switch (pkgType) {
      case 0: return decodeRpcRequest(frameData, traceId, spanId, pkgId, pkgFlags);
      case 1: return decodeRpcResponse(frameData, traceId, spanId, pkgId, pkgFlags);
      case 2: return decodeRpcEvent(frameData, traceId, spanId, pkgId, pkgFlags);
      case 3: // CONTROL
        break;
    }
    throw new UnsupportedOperationException();
  }

  public static void encodeRpc(final DnacoRpcPacket packet, final ByteBuf out) {
    final int pkgIdLen = IntUtil.size(packet.getPacketId());

    final int rpcHead = (packet.getPacketType().ordinal() << 6) | (pkgIdLen - 1);
    out.writeByte(rpcHead);
    out.writeLong(packet.getTraceId().getHi());
    out.writeLong(packet.getTraceId().getLo());
    out.writeLong(packet.getSpanId().getSpanId());
    writeLong(out, pkgIdLen, packet.getPacketId());

    switch (packet.getPacketType()) {
      case REQUEST:
        encodeRpcRequest((DnacoRpcRequest)packet, out);
        return;
      case RESPONSE:
        encodeRpcResponse((DnacoRpcResponse)packet, out);
        return;
      case EVENT:
        encodeRpcEvent((DnacoRpcEvent)packet, out);
        return;
      case CONTROL:
        break;
    }
  }

  private static DnacoRpcPacket decodeRpcRequest(final ByteBuf in, final TraceId traceId, final SpanId spanId,
      final long pkgId, final int pkgFlags) {
    // RPC Request Header packets are composed of:
    // - Send Result to: 2bit (CALLER, STORE_IN_MEMORY, STORE_WITH_ID, FORWARD_TO)
    // - Request Id Length: 7bit (1 + (0-127)) max 128bytes string
    // - Result Id Length: 7bit (1 + (0-127)) max 128bytes string.  used only when Send Result To is not CALLER.
    //   +----+---------+--------+ +-------------+ +-------------+
    //   | 11 | 1111111 | 111111 | | Request Id  | |  Result Id  |
    //   +----+---------+--------+ +-------------+ +-------------+
    //   0    2         9       16  (1-128 bytes)   (1-128 bytes)
    final int reqHead = in.readShort() & 0xffff;
    final int sendResultTo = (reqHead >> 14) & 0x3;
    final int requestIdLen = 1 + ((reqHead >> 7) & 0x7f);
    final int resultIdLen = sendResultTo != 0 ? 1 + (reqHead & 0x7f) : 0;
    Logger.debug("reqHead:{} sendResultTo:{} requestIdLen:{} resultIdLen:{}",
      reqHead, sendResultTo, requestIdLen, resultIdLen);
    final ByteBuf requestId = in.readRetainedSlice(requestIdLen);
    final ByteBuf resultId = in.readRetainedSlice(resultIdLen);
    final ByteBuf data = in.retainedSlice();
    Logger.debug("data:{} -> {}", data.readableBytes(), data.toString(StandardCharsets.UTF_8));
    return DnacoRpcRequest.alloc(traceId, spanId, pkgId, requestId, data, sendResultTo, resultId);
  }

  private static void encodeRpcRequest(final DnacoRpcRequest request, final ByteBuf out) {
    final int reqHead = (request.getSendResultTo().ordinal() << 14)
                      | ((request.getRequestId().readableBytes() - 1) << 7)
                      | (request.hasResultId() ? (request.getResultId().readableBytes() - 1) : 0);
    out.writeShort(reqHead);
    out.writeBytes(request.getRequestId());
    if (request.hasResultId()) {
      out.writeBytes(request.getResultId());
    }
    out.writeBytes(request.getData());
  }

  private static DnacoRpcPacket decodeRpcResponse(final ByteBuf in, final TraceId traceId, final SpanId spanId,
      final long pkgId, final int pkgFlags) {
    // RPC Response Header Packets are composed of:
    // - Operation Status: 2bit (SUCCEEDED, FAILED, CANCELLED, _)
    // - Queue Time length: 3bit (1 + (0-7)) max 8bytes int
    // - Exec Time length: 3bit (1 + (0-7)) max 8bytes int
    //   +----+-----+-----+ +---------------+ +---------------+
    //   | 11 | 111 | 111 | | Queue Time ns | | Exec Time ns  |
    //   +----+-----+-----+ +---------------+ +---------------+
    //   0    2     5     8    (1-8 bytes)       (1-8 bytes)
    final int respHead = in.readByte() & 0xff;
    final int opStatus = (respHead >> 6) & 0x3;
    final int queueTimeLen = 1 + ((respHead >> 3) & 0x7);
    final int execTimeLen = 1 + (respHead & 0x7);
    final long queueTime = readLong(in, queueTimeLen);
    final long execTime = readLong(in, execTimeLen);
    final ByteBuf data = in.retainedSlice();
    return DnacoRpcResponse.alloc(traceId, spanId, pkgId, opStatus, queueTime, execTime, data);
  }

  private static void encodeRpcResponse(final DnacoRpcResponse response, final ByteBuf out) {
    final int queueTimeLen = Math.max(1, IntUtil.size(response.getQueueTime()));
    final int execTimeLen = Math.max(1, IntUtil.size(response.getExecTime()));

    final int respHead = (response.getOperationStatus().ordinal() << 6) | ((queueTimeLen - 1) << 3) | (execTimeLen - 1);
    out.writeByte(respHead);
    writeLong(out, queueTimeLen, response.getQueueTime());
    writeLong(out, execTimeLen, response.getExecTime());
    out.writeBytes(response.getData());
  }

  private static DnacoRpcPacket decodeRpcEvent(final ByteBuf in, final TraceId traceId, final SpanId spanId,
      final long pkgId, final int pkgFlags) {
    // RPC Event Header Packets are composed of:
    // A Variable-Length Integer containing the length of the EventId. 1byte allows max 240 eventId length
    // +----------+ +----------+
    // | 11111111 | | Event Id |
    // +----------+ +----------+
    // 0          8
    final int eventIdLen = in.readByte() & 0xff;
    if (eventIdLen > 240) throw new UnsupportedOperationException();

    final ByteBuf eventId = in.readRetainedSlice(eventIdLen);
    final ByteBuf data = in.retainedSlice();
    return DnacoRpcEvent.alloc(traceId, spanId, pkgId, pkgFlags, eventId, data);
  }

  private static void encodeRpcEvent(final DnacoRpcEvent event, final ByteBuf out) {
    out.writeByte(event.getEventId().readableBytes());
    out.writeBytes(event.getEventId());
    out.writeBytes(event.getData());
  }

  private static long readLong(final ByteBuf in, final int bytesWidth) {
    long result = 0;
    for (int i = 0; i < bytesWidth; ++i) {
      result += (((long)in.readByte() & 0xff) << (i << 3));
    }
    return result;
  }

  private static void writeLong(final ByteBuf out, final int bytesWidth, final long value) {
    for (int i = 0; i < bytesWidth; ++i) {
      out.writeByte((int) ((value >>> (i << 3)) & 0xff));
    }
  }
}