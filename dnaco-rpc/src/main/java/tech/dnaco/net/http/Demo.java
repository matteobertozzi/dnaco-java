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

import java.util.Arrays;
import java.util.Date;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.http.DnacoHttpService.DnacoHttpServiceProcessor;
import tech.dnaco.net.http.HttpDispatcher.HttpTask;
import tech.dnaco.net.http.HttpHandler.UriPrefix;
import tech.dnaco.net.message.DnacoMessage;

public class Demo {
  private static class DemoProcessor implements DnacoHttpServiceProcessor {
    private final HttpDispatcher dispatcher;

    private DemoProcessor(final HttpDispatcher dispatcher)  {
      this.dispatcher = dispatcher;
    }

    @Override
    public void sessionMessageReceived(final ChannelHandlerContext ctx, final FullHttpRequest msg) throws Exception {
      //System.out.println("HTTP MSG RECEIVED: " + HumansUtil.humanSize(msg.content().readableBytes()));

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

  @UriPrefix("/foo")
  public static class Foo implements HttpHandler {
    @UriVariableMapping(uri = "/test/{p1}/{p2}", method = HttpMethod.POST)
    public JsonObject foo(
        @QueryParam(name = "a") final int intValue,
        @QueryParam("s") final String[] sValues,
        @QueryParam("x") final String xValue,
        @UriVariable("p1") final int p1,
        @UriVariable(name = "p2") final String p2,
        @HeaderValue("X-Foo") final int hIntValue,
        @HeaderValue(name = "X-Bar") final String hStrValue,
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

    @UriMapping(uri = "/")
    public String hello() {
      return "hello, world!";
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

    final HttpRouters.UriRoutesBuilder routes = new HttpRouters.UriRoutesBuilder();
    //routes.addStaticFileHandler("/static", new File("/Users/th30z/tmp"));
    //routes.addStaticFileHandler("/foo", new File("/Users/th30z/tmp/bar"));
    //routes.addStaticFileHandler("/stat/(.*)/bar", new File("/Users/th30z/tmp"));
    routes.addHandler(new Foo());

    final HttpDispatcher dispatcher = new HttpDispatcher(routes);
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
