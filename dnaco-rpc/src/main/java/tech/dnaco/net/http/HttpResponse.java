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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

public class HttpResponse {
  private final HttpResponseStatus status;
  private final HttpHeaders headers;
  private final ByteBuf body;

  public HttpResponse(final HttpResponseStatus status, final HttpHeaders headers, final String body) {
    this(status, headers, body.getBytes());
  }

  public HttpResponse(final HttpResponseStatus status, final HttpHeaders headers, final byte[] body) {
    this(status, headers, Unpooled.wrappedBuffer(body));
  }

  public HttpResponse(final HttpResponseStatus status, final HttpHeaders headers, final ByteBuf body) {
    this.status = status;
    this.headers = headers;
    this.body = body;
  }

  public static HttpResponse ok(final String body) {
    return ok(body.getBytes());
  }

  public static HttpResponse ok(final byte[] body) {
    return new HttpResponse(HttpResponseStatus.OK, EmptyHttpHeaders.INSTANCE, body);
  }

  public static HttpResponse html(final HttpResponseStatus status, final String body) {
    return new HttpResponse(status, EmptyHttpHeaders.INSTANCE, body);
  }

  public HttpResponseStatus getStatus() {
    return status;
  }

  public HttpHeaders getHeaders() {
    return headers;
  }

  public ByteBuf getBody() {
    return body;
  }
}
