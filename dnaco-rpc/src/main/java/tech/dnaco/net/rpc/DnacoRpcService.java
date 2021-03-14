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

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import tech.dnaco.net.AbstractService;
import tech.dnaco.net.frame.DnacoFrameDecoder;
import tech.dnaco.net.frame.DnacoFrameEncoder;
import tech.dnaco.tracing.Span;
import tech.dnaco.tracing.TraceAttributes;
import tech.dnaco.tracing.Tracer;

public class DnacoRpcService extends AbstractService {
  private final DnacoRpcServiceHandler handler;

  public DnacoRpcService(final DnacoRpcDispatcher dispatcher) {
    this.handler = new DnacoRpcServiceHandler(dispatcher);
  }

  @Override
  protected void setupPipeline(final ChannelPipeline pipeline) {
    //pipeline.addLast(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
    //pipeline.addLast(ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
    pipeline.addLast(DnacoFrameEncoder.INSTANCE);
    pipeline.addLast(DnacoRpcPacketEncoder.INSTANCE);
    pipeline.addLast(new DnacoFrameDecoder());
    pipeline.addLast(new DnacoRpcPacketDecoder());
    pipeline.addLast(handler);
  }

  @Sharable
  private static final class DnacoRpcServiceHandler extends ServiceChannelInboundHandler<DnacoRpcPacket> {
    private final DnacoRpcDispatcher dispatcher;

    private DnacoRpcServiceHandler(final DnacoRpcDispatcher dispatcher) {
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
    protected void channelRead0(final ChannelHandlerContext ctx, final DnacoRpcPacket packet) throws Exception {
      final DnacoRpcSession session = getSession(ctx.channel());
      try (Span span = Tracer.newSubTask(packet.getTraceId(), packet.getSpanId())) {
        span.setAttribute(TraceAttributes.LABEL, "handle client packets");
        span.setAttribute(TraceAttributes.MODULE, getClass().getSimpleName());

        dispatcher.handlePacket(session, packet);
      }
    }
  }
}
