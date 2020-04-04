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

package tech.dnaco.storage.service.http;

import java.lang.reflect.InvocationTargetException;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import tech.dnaco.logging.LogUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.server.http.HttpResponseUtil;
import tech.dnaco.server.http.HttpRouter;
import tech.dnaco.server.http.HttpRouter.HttpRoute;
import tech.dnaco.server.http.HttpService.HttpListener;

public class HttpDispatcher implements HttpListener {
	private final HttpRouter router = new HttpRouter();

	public HttpDispatcher() {
		router.addHandler(new HttpStorageHandler());
	}

	@Override
	public void requestReceived(final ChannelHandlerContext ctx, final FullHttpRequest request) {
		final QueryStringDecoder query = new QueryStringDecoder(request.uri());

		final HttpRoute route = router.get(request.method(), query.path());
		if (route == null) {
			HttpResponseUtil.writeJsonResponse(ctx, HttpResponseStatus.NOT_FOUND,
				new HttpError("URI_NOT_FOUND", request.method() + " " + request.uri() + " not found"));
			return;
		}

		try {
			final Object result = route.call(ctx, request);
			if (result != null) {
				HttpResponseUtil.writeJsonResponse(ctx, HttpResponseStatus.OK, result);
			}
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			HttpResponseUtil.writeJsonResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
				new HttpError("UNABLE_TO_INVOKE", e.getMessage()));
		} catch (Throwable e) {
			HttpResponseUtil.writeJsonResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
				new HttpError("UNHANDLED_ERROR", e.getMessage()));
		}
	}

  private static final class HttpError {
    private final String status;
    private final String message;
    private final String traceId;

    private HttpError(final String status, final String message) {
      this.status = status;
      this.message = message;
      this.traceId = LogUtil.toTraceId(Logger.getSessionTraceId());
    }
  }
}