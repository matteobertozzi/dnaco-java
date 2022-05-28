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

import java.util.Date;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.dispatcher.message.MessageMetadataMap;
import tech.dnaco.dispatcher.message.UriRouters;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.http.DnacoHttpService.DnacoHttpServiceProcessor;
import tech.dnaco.net.http.HttpDispatcher.HttpTask;
import tech.dnaco.net.message.DnacoMessage;
import tech.dnaco.net.message.DnacoMessageHttpEncoder;
import tech.dnaco.net.message.DnacoMessageUtil;
import tech.dnaco.strings.HumansUtil;

public class Demo {
  private static class DemoProcessor implements DnacoHttpServiceProcessor {
    private final HttpDispatcher dispatcher;

    private DemoProcessor(final HttpDispatcher dispatcher)  {
      this.dispatcher = dispatcher;
    }

    @Override
    public void sessionMessageReceived(final ChannelHandlerContext ctx, final FullHttpRequest msg) throws Exception {
      //System.out.println("HTTP MSG RECEIVED: " + HumansUtil.humanSize(msg.content().readableBytes()));

      if (false) {
        final ByteBuf body = Unpooled.wrappedBuffer(JsonUtil.toJson(new String[] { "aaa", "bbb", "ccc" }).getBytes());
        if (false) {
          final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
          response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 19);
          response.headers().set(HttpHeaderNames.SERVER, "Armeria/1.16.0");
          response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
          response.headers().set(HttpHeaderNames.DATE, new Date());
          ctx.write(response);
        } else {
          final DnacoMessage message = new DnacoMessage(0L, new MessageMetadataMap()
            .add(DnacoMessageUtil.METADATA_FOR_HTTP_STATUS, 200)
            .add("content-length", 19)
            .add("server", "Armeria/1.16.0")
            .add("content-type", "application/json; charset=utf-8")
            .add("date", "Mon, 23 May 2022 09:27:56 GMT"),
            body);
          ctx.write(message);
        }
        return;
      }

      final HttpTask task = dispatcher.prepare(msg);
      if (task != null) {
        final DnacoMessage message = task.execute();
        ctx.write(message);
        return;
      }

      if (dispatcher.serveStaticFileHandler(ctx, msg)) {
        return;
      }

      final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, Unpooled.EMPTY_BUFFER);
      response.headers().set(HttpHeaderNames.DATE, new Date());
      if (HttpUtil.isKeepAlive(msg)) {
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      } else {
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
      }
      ctx.write(response);
    }
  }

  public static class Foo implements HttpHandler {
    /*
    @UriVariableMapping(uri = "/test/{p1}/{p2}", method = UriMethod.POST)
    public JsonObject foo(
        @QueryParam("a") final int intValue,
        @QueryParam("s") final String[] sValues,
        @QueryParam("x") final String xValue,
        @UriVariable("p1") final int p1,
        @UriVariable("p2") final String p2,
        @HeaderValue("X-Foo") final int hIntValue,
        @HeaderValue("X-Bar") final String hStrValue,
        final JsonObject data) {
      System.out.println("/foo/test");
      System.out.println(" - p1: " + p1);
      System.out.println(" - p2: " + p2);
      System.out.println("header(X-Foo): " + hIntValue);
      System.out.println("header(X-Bar): " + hStrValue);
      System.out.println("queryParam('a'): " + intValue);
      System.out.println("queryParam('s'): " + Arrays.toString(sValues));
      System.out.println("queryParam('x'): " + xValue);
      System.out.println("body: " + data);
      return new JsonObject().add("foo", 10);
    }

    @UriMapping(uri = "/aaa")
    public JsonObject aaaa() {
      return new JsonObject().add("x", 10);
    }
*/
    @UriMapping(uri = "/")
    public String hello() {
      return "hello, world!";
    }

    @UriMapping(uri = "/bar")
    public String[] helloJson() {
      return new String[] { "aaa", "bbb", "ccc" };
    }
  }

  /*
  private static void buildServiceMap(final HttpRouters.UriRoutesBuilder routes ) {
    this.staticUriRouter = new UriStaticRouter<>(routes.getStaticUri(), this::buildUriHandler);
    this.variableUriRouter = new UriVariableRouter<>(routes.getVariableUri(), this::buildUriHandler);
    this.patternUriRouter = new UriPatternRouter<>(routes.getPatternUri(), this::buildUriHandler);
  }

  private static void buildServiceMap(UriRoute route) {
    //route.getHttpMethods()
    //route.getUri();
    route.getMethod().getAnnotation(annotationClass)
  }
  */

  public static void main(final String[] args) throws Exception {
    Logger.setDefaultLevel(LogLevel.INFO);

    final UriRouters.UriRoutesBuilder routes = new UriRouters.UriRoutesBuilder();
    //routes.addStaticFileHandler("/static", new File("/Users/th30z/tmp"));
    //routes.addStaticFileHandler("/foo", new File("/Users/th30z/tmp/bar"));
    //routes.addStaticFileHandler("/stat/(.*)/bar", new File("/Users/th30z/tmp"));
    routes.addHandler(new Foo());

    final HttpDispatcher dispatcher = new HttpDispatcher(routes);
    if (false) {
      final int N = 1_000_000;
      final long startTime = System.nanoTime();
      for (int i = 0; i < N; ++i) {
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/bar");
        final HttpTask task = dispatcher.prepare(request);
        final DnacoMessage message = task.execute();
        DnacoMessageHttpEncoder.encode(message).release();
        message.release();
      }
      final long elapsed = System.nanoTime() - startTime;

      System.out.println(HumansUtil.humanTimeNanos(elapsed) + " -> " + HumansUtil.humanRate((double)N / (elapsed / 1000000000.0)));
      return;
    }


    //final HttpTask task = dispatcher.prepare("POST", "/foo/test/123/x2?a=10&s=aaa&s=bbb&s=ccc&x=foo");
    //task.setMessage(List.of(Map.entry("X-Foo", "10"), Map.entry("X-Bar", "xx")), Unpooled.wrappedBuffer("{\"x\": 10}".getBytes()));

    try (ServiceEventLoop eventLoop = new ServiceEventLoop(1, 8)) {
      final DnacoHttpService service = new DnacoHttpService(new DemoProcessor(dispatcher));
      service.bindTcpService(eventLoop, 55123);
      service.addShutdownHook();
      service.waitStopSignal();
    }
  }
}
