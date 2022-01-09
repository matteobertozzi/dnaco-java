/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.net.http;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import tech.dnaco.data.util.MimeUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.util.UriUtil;
import tech.dnaco.strings.HumansUtil;

public final class HttpStaticFilesUtil {
  public static boolean ALLOW_ONLY_GET = false; // BODGE for the MDC-SyncServer

  private static final int CHUNK_SIZE = 1 << 20;

  private HttpStaticFilesUtil() {
    // no-op
  }

  public static boolean validateRequest(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    if (ALLOW_ONLY_GET && HttpMethod.GET.equals(request.method())) {
      HttpResponseUtil.writeEmptyResponse(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
      return false;
    }
    return true;
  }

  public static String sanitizePath(final ChannelHandlerContext ctx, final FullHttpRequest request, final String uriPrefix) {
    final QueryStringDecoder uriDecoder = new QueryStringDecoder(request.uri());
    final String path = UriUtil.sanitizeUri(uriPrefix, uriDecoder.path());
    if (path == null) {
      Logger.warn("forbidden file request {} uriPrefix {} - sanitization", request.uri(), uriPrefix);
      HttpResponseUtil.writeEmptyResponse(ctx, HttpResponseStatus.FORBIDDEN);
      return null;
    }
    return path;
  }

  public static void serveFile(final ChannelHandlerContext ctx, final FullHttpRequest request,
      final String uriPrefix, final File staticFileDir) throws IOException {
    serveFile(ctx, request, uriPrefix, staticFileDir, null, StatsCompletionListener.INSTANCE);
  }

  public static void serveFile(final ChannelHandlerContext ctx, final FullHttpRequest request, final String uriPrefix,
      final File staticFileDir, final HttpHeaders headers, final CompletionListener completionListener) throws IOException {
    if (!validateRequest(ctx, request)) return;

    final String path = sanitizePath(ctx, request, uriPrefix);
    if (path == null) return;

    serveFile(ctx, request, new File(staticFileDir, path), headers, completionListener);
  }

  public static void serveFile(final ChannelHandlerContext ctx, final FullHttpRequest request, final File file) throws IOException {
    serveFile(ctx, request, file, null, StatsCompletionListener.INSTANCE);
  }

  public static void serveFile(final ChannelHandlerContext ctx, final FullHttpRequest request,
      final File file, final HttpHeaders headers) throws IOException {
    serveFile(ctx, request, file, headers, StatsCompletionListener.INSTANCE);
  }


  public static void serveFile(final ChannelHandlerContext ctx, final FullHttpRequest request,
      final File file, final CompletionListener completionListener) throws IOException {
    serveFile(ctx, request, file, null, completionListener);
  }

  public static void serveFile(final ChannelHandlerContext ctx, final FullHttpRequest request, final File file,
      final HttpHeaders headers, final CompletionListener completionListener) throws IOException {
    if (file.isHidden() || !file.exists()) {
      Logger.warn("the file requested {} is {}", file, file.isHidden() ? "hidden" : "not existent");
      HttpResponseUtil.writeEmptyResponse(ctx, HttpResponseStatus.NOT_FOUND);
      return;
    }

    if (!file.isFile()) {
      //Logger.warn("{} forbidden file request {}, not a file", ctx.getRemoteAddress(), file);
      Logger.warn("forbidden file request {}, not a file", file);
      HttpResponseUtil.writeEmptyResponse(ctx, HttpResponseStatus.FORBIDDEN);
      return;
    }

    doServeFile(ctx, request, file, headers, completionListener);
  }

  public static void doServeFile(final ChannelHandlerContext ctx, final FullHttpRequest request, final File file,
      final HttpHeaders headers, final CompletionListener completionListener) throws IOException {
    final long startTime = System.nanoTime();

    final RandomAccessFile raf;
    try {
      raf = new RandomAccessFile(file, "r");
    } catch (final FileNotFoundException e) {
      Logger.error(e, "file {} not found", file);
      HttpResponseUtil.writeEmptyResponse(ctx, HttpResponseStatus.NOT_FOUND);
      return;
    }

    final long fileLength = raf.length();
    Logger.info("Reading file {} length {}", file, fileLength);
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    if (headers != null && !headers.isEmpty()) {
      for (final Map.Entry<String, String> entry: headers.entries()) {
        response.headers().add(entry.getKey(), entry.getValue());
      }
    }

    HttpUtil.setContentLength(response, fileLength);
    if (!response.headers().contains(HttpHeaderNames.CONTENT_TYPE)) {
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, MimeUtil.INSTANCE.detectMimeType(file));
    }
    if (HttpUtil.isKeepAlive(request)) {
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    }

    // Write the initial line and the header.
    ctx.write(response, ctx.channel().voidPromise());

    // Write the content.
    final ChannelFuture sendFileFuture;
    final ChannelFuture lastContentFuture;
    if (ctx.pipeline().get(SslHandler.class) == null) {
      if (!response.headers().contains(HttpHeaderNames.CONTENT_ENCODING)) {
        // Forcefully disable the content compressor as it cannot compress a DefaultFileRegion
        response.headers().set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.IDENTITY);
      }

      sendFileFuture = ctx.write(
        new DefaultFileRegion(raf.getChannel(), 0, fileLength),
        ctx.newProgressivePromise());

      // Write the end marker.
      lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
    } else {
      sendFileFuture = ctx.writeAndFlush(
        new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, CHUNK_SIZE)),
        ctx.newProgressivePromise());
      // HttpChunkedInput will write the end marker (LastHttpContent) for us.
      lastContentFuture = sendFileFuture;
    }

    sendFileFuture.addListener(new StatsProgressListener(file, startTime, completionListener));

    // Decide whether to close the connection or not.
    if (!HttpUtil.isKeepAlive(request)) {
      // Close the connection when the whole content is written out.
      lastContentFuture.addListener(ChannelFutureListener.CLOSE);
    }
  }

  private static final class StatsProgressListener implements ChannelProgressiveFutureListener {
    private final CompletionListener completionListener;
    private final long startTime;
    private final File file;

    private StatsProgressListener(final File file, final long startTime, final CompletionListener completionListener) {
      this.file = file;
      this.startTime = startTime;
      this.completionListener = completionListener;
    }

    @Override
    public void operationProgressed(final ChannelProgressiveFuture future, final long progress, final long total) {
      if (total < 0) { // total unknown
        Logger.trace("{} Transfer progress: {}", future.channel(), progress);
      } else {
        Logger.trace("{} Transfer progress: {} / {}", future.channel(), progress, total);
      }
    }

    @Override
    public void operationComplete(final ChannelProgressiveFuture future) {
      final long elapsed = System.nanoTime() - startTime;
      Logger.debug("{} {} Transfer complete {}.", future.channel(), file.toPath(),
        HumansUtil.humanTimeNanos(elapsed));
      completionListener.completed(file, elapsed);
    }
  }

  public interface CompletionListener {
    void completed(final File file, long elapsed);
  }

  public static final class StatsCompletionListener implements CompletionListener {
    public static final StatsCompletionListener INSTANCE = new StatsCompletionListener();

    private StatsCompletionListener() {
      // no-op
    }

    @Override
    public void completed(final File file, final long elapsed) {
      //ServerStaticFileStats.getInstance().incStaticFileRequests(elapsed, file.length(), file);
    }
  }

  public static final class NoopCompletionListener implements CompletionListener {
    public static final NoopCompletionListener INSTANCE = new NoopCompletionListener();

    private NoopCompletionListener() {
      // no-op
    }

    @Override
    public void completed(final File file, final long elapsed) {
      // no-op
    }
  }
}
