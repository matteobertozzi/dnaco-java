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

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.AbstractService;
import tech.dnaco.net.frame.DnacoFrame;
import tech.dnaco.net.frame.DnacoFrameUtil;
import tech.dnaco.tracing.Span;
import tech.dnaco.tracing.TraceAttributes;
import tech.dnaco.tracing.Tracer;

public class DnacoRpcWsService extends AbstractService {
  private final WebSocketFrameHandler handler;
  private final String wsPath;
  private final int maxFrameSize;

  public DnacoRpcWsService(final DnacoRpcDispatcher dispatcher, final String wsPath) {
    this(dispatcher, wsPath, DnacoFrameUtil.MAX_FRAME_SIZE);
  }

  public DnacoRpcWsService(final DnacoRpcDispatcher dispatcher, final String wsPath, final int maxFrameSize) {
    this.wsPath = wsPath;
    this.maxFrameSize = maxFrameSize;
    this.handler = new WebSocketFrameHandler(dispatcher);
  }

	@Override
	protected void setupPipeline(final ChannelPipeline pipeline) {
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpObjectAggregator(maxFrameSize));
    pipeline.addLast(new WebSocketServerCompressionHandler());
    pipeline.addLast(new WebSocketServerProtocolHandler(wsPath, null, true));
    pipeline.addLast(DnacoFrameToBinaryWebSocketFrameEncoder.INSTANCE);
    pipeline.addLast(DnacoRpcPacketEncoder.INSTANCE);
    pipeline.addLast(handler);
  }

  @Sharable
  private static final class DnacoFrameToBinaryWebSocketFrameEncoder extends MessageToMessageEncoder<DnacoFrame> {
    private static final DnacoFrameToBinaryWebSocketFrameEncoder INSTANCE = new DnacoFrameToBinaryWebSocketFrameEncoder();

    @Override
    protected void encode(final ChannelHandlerContext ctx, final DnacoFrame frame, final List<Object> out) throws Exception {
      final ByteBuf buf = ctx.alloc().buffer();
      DnacoFrameUtil.encodeFrame(frame, buf);
      Logger.debug("dnaco frame to binary web sock frame: {} {}", frame, ByteBufUtil.hexDump(buf));
      out.add(new BinaryWebSocketFrame(buf));
    }
  }

  @Sharable
  private static final class WebSocketFrameHandler extends ServiceChannelInboundHandler<WebSocketFrame> {
    private final DnacoRpcDispatcher dispatcher;

    public WebSocketFrameHandler(final DnacoRpcDispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }

    @Override
    protected AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) {
      return dispatcher.sessionConnected(ctx);
    }

    @Override
    protected void sessionDisconnected(final AbstractServiceSession session) {
      dispatcher.sessionDisconnected(session);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final WebSocketFrame frame) throws Exception {
      if (frame instanceof BinaryWebSocketFrame) {
        processBinaryFrame(ctx, (BinaryWebSocketFrame)frame);
      } else if (frame instanceof TextWebSocketFrame) {
        processTextFrame(ctx, (TextWebSocketFrame)frame);
      } else {
        final String message = "unsupported frame type: " + frame.getClass().getName();
        throw new UnsupportedOperationException(message);
      }
    }

    private void processBinaryFrame(final ChannelHandlerContext ctx, final BinaryWebSocketFrame wsFrame) {
      final DnacoRpcSession session = getSession(ctx.channel());
      DnacoRpcPacketUtil.decode(wsFrame.content(), (packet) -> {

        try (Span span = Tracer.newSubTask(packet.getTraceId(), packet.getSpanId())) {
          span.setLabel("handle client packet");
          span.setAttribute(TraceAttributes.MODULE, getClass().getSimpleName());
          Logger.debug("process binary: {}", packet);
          dispatcher.handlePacket(session, packet);
        }
      });
    }

    private void processTextFrame(final ChannelHandlerContext ctx, final TextWebSocketFrame frame) {
      Logger.debug("process text: {}", frame.text());
      ctx.write(frame.retain());
    }
  }
}
