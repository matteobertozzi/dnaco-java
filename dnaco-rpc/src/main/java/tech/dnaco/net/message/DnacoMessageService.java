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

package tech.dnaco.net.message;

import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import tech.dnaco.net.AbstractService;
import tech.dnaco.net.frame.DnacoFrameDecoder;
import tech.dnaco.net.frame.DnacoFrameEncoder;

public class DnacoMessageService extends AbstractService {
  private final DnacoMessageHandler handler;

  public DnacoMessageService(final DnacoMessageServiceProcessor processor) {
    this.handler = new DnacoMessageHandler(processor, running());
  }

  @Override
  protected void setupPipeline(final ChannelPipeline pipeline) {
    pipeline.addLast(new DnacoFrameDecoder());
    pipeline.addLast(DnacoMessageDecoder.INSTANCE);
    pipeline.addLast(DnacoFrameEncoder.INSTANCE);
    pipeline.addLast(DnacoMessageEncoder.INSTANCE);
    pipeline.addLast(handler);
  }

  @Sharable
  private static class DnacoMessageHandler extends ServiceChannelInboundHandler<DnacoMessage> {
    private final DnacoMessageServiceProcessor processor;
    private final AtomicBoolean running;

    private DnacoMessageHandler(final DnacoMessageServiceProcessor processor, AtomicBoolean running) {
      this.processor = processor;
      this.running = running;
    }

    @Override
    protected AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) {
      return processor.sessionConnected(ctx);
    }

    @Override
    protected void sessionDisconnected(final AbstractServiceSession session) {
      processor.sessionDisconnected(session);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DnacoMessage msg) throws Exception {
      if (running.get()) {
        processor.sessionMessageReceived(ctx, msg);
      } else {
        ctx.close();
      }
    }
  }

  public interface DnacoMessageServiceProcessor {
    default AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) { return null; }
    default void sessionDisconnected(final AbstractServiceSession session) {}

    void sessionMessageReceived(ChannelHandlerContext ctx, DnacoMessage message) throws Exception;
  }
}
