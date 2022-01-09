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
import java.io.IOException;
import java.util.regex.Matcher;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

public class HttpStaticFileHandler implements HttpHandler, HttpStaticFilesUtil.CompletionListener {
  private final File staticFileDir;
  private final String uriPrefix;

  public HttpStaticFileHandler(final String uriPrefix, final File staticFileDir) {
    this.uriPrefix = uriPrefix;
    this.staticFileDir = staticFileDir;
  }

  public void handleStaticFileRequest(final ChannelHandlerContext ctx, final Matcher uriMatcher,
      final FullHttpRequest request) throws IOException {
    if (uriMatcher.groupCount() > 1) {
      System.out.println("MATCHER " + uriMatcher + " -> " + uriMatcher.groupCount());
      final StringBuilder buf = new StringBuilder(uriPrefix.length());
      while (uriMatcher.find()) {
        final String key = uriMatcher.group(1);
        uriMatcher.appendReplacement(buf, Matcher.quoteReplacement(key));
      }
      uriMatcher.appendTail(buf);
      System.out.println(" -> REPLACED URI-PREFIX -> " + buf);
    }
    HttpStaticFilesUtil.serveFile(ctx, request, uriPrefix, staticFileDir, null, this);
  }

  @Override
  public void completed(final File file, final long elapsed) {
    //ServerStaticFileStats.getInstance().incStaticFileRequests(elapsed, file.length(), file);
  }
}
