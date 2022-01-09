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
package tech.dnaco.net.http;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import tech.dnaco.net.AbstractService;
import tech.dnaco.net.message.DnacoMessageHttpEncoder;

public class DnacoHttpService extends AbstractService {
  private static final int MAX_HTTP_REQUEST_SIZE = (4 << 20);

  private static final String X_HEADER_TRACE_ID = "X-TraceId";
  private static final String X_HEADER_SPAN_ID = "X-SpanId";
  private static final String X_HEADER_TOTAL_TIME = "X-TotalTime";

  private final HttpFrameHandler handler;
  private final CorsConfig corsConfig;

  public DnacoHttpService(final DnacoHttpServiceProcessor processor) {
    this(processor, false);
  }

  public DnacoHttpService(final DnacoHttpServiceProcessor processor, final boolean enableCors) {
    this.handler = new HttpFrameHandler(processor);

    if (enableCors) {
      this.corsConfig = CorsConfigBuilder.forAnyOrigin().allowNullOrigin()
        .maxAge(3600)
        .allowCredentials()
        .allowedRequestMethods(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH, HttpMethod.DELETE)
        .allowedRequestHeaders("*")
        .exposeHeaders(X_HEADER_TRACE_ID, X_HEADER_SPAN_ID, X_HEADER_TOTAL_TIME)
        .build();
    } else {
      this.corsConfig = null;
    }
  }

	@Override
	protected void setupPipeline(final ChannelPipeline pipeline) {
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpContentDecompressor());
    pipeline.addLast(new HttpServerKeepAliveHandler());
    pipeline.addLast(new HttpServerExpectContinueHandler());
    pipeline.addLast(new HttpObjectAggregator(MAX_HTTP_REQUEST_SIZE));
    pipeline.addLast(new SmartHttpContentCompressor());
    pipeline.addLast(new ChunkedWriteHandler());
    if (corsConfig != null) {
      pipeline.addLast(new CorsHandler(corsConfig));
    }
    pipeline.addLast(DnacoMessageHttpEncoder.INSTANCE);
    pipeline.addLast(handler);
  }

  @Sharable
  private static final class HttpFrameHandler extends ServiceChannelInboundHandler<FullHttpRequest> {
    private final DnacoHttpServiceProcessor processor;

    private HttpFrameHandler(final DnacoHttpServiceProcessor processor) {
      this.processor = processor;
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
    protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest msg) throws Exception {
      processor.sessionMessageReceived(ctx, msg);
    }
  }

  public interface DnacoHttpServiceProcessor {
    default AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) { return null; }
    default void sessionDisconnected(final AbstractServiceSession session) {}

    void sessionMessageReceived(ChannelHandlerContext ctx, FullHttpRequest message) throws Exception;
  }
}
