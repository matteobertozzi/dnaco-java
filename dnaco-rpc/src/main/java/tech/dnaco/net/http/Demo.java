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

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

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
import io.netty.handler.codec.http.HttpVersion;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.dispatcher.Actions.AsyncResult;
import tech.dnaco.dispatcher.message.Message;
import tech.dnaco.dispatcher.message.MessageError;
import tech.dnaco.dispatcher.message.MessageException;
import tech.dnaco.dispatcher.message.MessageUtil;
import tech.dnaco.dispatcher.message.UriDispatcher.MessageTask;
import tech.dnaco.dispatcher.message.UriRouters;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.http.DnacoHttpService.DnacoHttpServiceProcessor;
import tech.dnaco.threading.BenchUtil;
import tech.dnaco.threading.ShutdownUtil;
import tech.dnaco.threading.ThreadUtil;

public class Demo {
  private static final byte[] TEST_DATA = "hello world".getBytes();

  private static final class StaticPerfProcessor implements DnacoHttpServiceProcessor {
    private StaticPerfProcessor(final HttpDispatcher dispatcher) {
      // no-op
    }

    @Override
    public void sessionMessageReceived(final ChannelHandlerContext ctx, final FullHttpRequest message) throws Exception {
      final ByteBuf body = Unpooled.wrappedBuffer(TEST_DATA);
      final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
      response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.readableBytes());
      response.headers().set(HttpHeaderNames.SERVER, "Armeria/1.16.0");
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
      response.headers().set(HttpHeaderNames.DATE, new Date());
      ctx.write(response);
      REQ_COUNT.increment();
    }
  }

  protected static class DefaultHttpProcessor implements DnacoHttpServiceProcessor {
    private final HttpDispatcher dispatcher;

    protected DefaultHttpProcessor(final HttpDispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }

    @Override
    public void sessionMessageReceived(final ChannelHandlerContext ctx, final FullHttpRequest req) throws Exception {
      REQ_COUNT.increment();
      System.out.println("MESSAGE ON THREAD " + Thread.currentThread().getName());
      final MessageTask task = dispatcher.prepare(req);
      if (task != null) {
        final Message message = task.execute();
        respondWithMessage(ctx, req, message);
        return;
      }

      System.out.println("SBANG!");
      if (dispatcher.serveStaticFileHandler(ctx, req)) {
        return;
      }

      respondWithNotFound(ctx, req);
    }

    protected void respondWithMessage(final ChannelHandlerContext ctx, final FullHttpRequest request, final Message response) {
      if (response instanceof final HttpMessageResponse httpResponse) {
        ctx.write(httpResponse.rawResponse());
      } else {
        System.out.println("SBONG!");
      }
    }

    protected void respondWithNotFound(final ChannelHandlerContext ctx, final FullHttpRequest msg) {
      final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT, Unpooled.EMPTY_BUFFER);
      response.headers().set(HttpHeaderNames.DATE, new Date());
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      response.headers().set("X-Thread", Thread.currentThread().getName());
      response.headers().set("X-Count", String.valueOf(REQ_COUNT.sum()));
      ctx.write(response);
      System.out.println(" ----> RESPOND " + Thread.currentThread());
    }
  }

  private static final LongAdder REQ_COUNT = new LongAdder();

  public static class Foo implements HttpHandler {
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

    @UriMapping(uri = "/void")
    public void testVoid() {
      //System.out.println("/void");
      REQ_COUNT.increment();
    }

    @AsyncResult
    @UriMapping(uri = "/async")
    public void testAsyncResult() {
      System.out.println("/async");
    }

    @UriMapping(uri = "/")
    public String hello() {
      return "hello, world!";
    }

    @UriMapping(uri = "/bar")
    public String[] helloJson() {
      return new String[] { "aaa", "bbb", "ccc" };
    }

    @UriMapping(uri = "/raw")
    public byte[] helloRaw() {
      return "hello-bytes".getBytes();
    }

    @UriMapping(uri = "/msg")
    public Message helloMsg() {
      return MessageUtil.newDataMessage(Map.of("X-Foo", "10"), List.of("a", "b", "c"));
    }

    @UriMapping(uri = "/except")
    public void helloExcept() throws MessageException {
      throw new MessageException(new MessageError(512, "boom", "hello, world!"));
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
    routes.addStaticFileHandler("/static", new File("/Users/th30z/tmp"));
    //routes.addStaticFileHandler("/foo", new File("/Users/th30z/tmp/bar"));
    //routes.addStaticFileHandler("/stat/(.*)/bar", new File("/Users/th30z/tmp"));
    routes.addHandler(new Foo());

    final HttpDispatcher dispatcher = new HttpDispatcher(routes);
    if (false) {
      BenchUtil.run("httpDispatcher", Duration.ofMinutes(5), () -> {
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/bar", false);
        final MessageTask task = dispatcher.prepare(request);
        final Message m = task.execute();
        if (m instanceof final HttpMessageResponse httpResponse) {
          httpResponse.rawResponse().release();
        }
      });
      return;
    }


    //final HttpTask task = dispatcher.prepare("POST", "/foo/test/123/x2?a=10&s=aaa&s=bbb&s=ccc&x=foo");
    //task.setMessage(List.of(Map.entry("X-Foo", "10"), Map.entry("X-Bar", "xx")), Unpooled.wrappedBuffer("{\"x\": 10}".getBytes()));

    final AtomicBoolean running = new AtomicBoolean(true);
    try (ServiceEventLoop eventLoop = new ServiceEventLoop(1, 8)) {
      //final DnacoHttpService service = new DnacoHttpService(new DefaultHttpProcessor(dispatcher));
      final DnacoHttpService service = new DnacoHttpService(new StaticPerfProcessor(dispatcher));
      service.bindTcpService(eventLoop, 55123);
      //service.addShutdownHook();
      ShutdownUtil.addShutdownHook("services", running, service);

      while (running.get()) {
        ThreadUtil.sleep(1, TimeUnit.SECONDS);
        //System.out.println("REQ COUNT: " + HumansUtil.humanCount(REQ_COUNT.longValue()));
      }

      service.waitStopSignal();
    }
  }
}
