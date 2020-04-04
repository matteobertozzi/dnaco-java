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

package tech.dnaco.server.http;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import tech.dnaco.logging.Logger;
import tech.dnaco.server.http.HttpRequestHandler.UriMapping;
import tech.dnaco.server.http.HttpRequestHandler.UriPatternMapping;
import tech.dnaco.server.http.HttpRequestHandler.UriVariableMapping;

public class HttpRouter {
  private final HashMap<String, HttpRoute> directRoutes = new HashMap<>();

  public HttpRoute get(final HttpMethod method, final String uri) {
    Logger.trace("checking for route: {} {}", method, uri);
    HttpRoute route = directRoutes.get(method.name() + uri);
    return route;
  }

  public void addHandler(final HttpRequestHandler handler) {
    final Method[] methods = handler.getClass().getMethods();
    if (methods == null || methods.length == 0) return;
    for (int i = 0; i < methods.length; ++i) {
      if (methods[i].isAnnotationPresent(UriMapping.class)) {
        addDirectMapping(handler, methods[i]);
      } else if (methods[i].isAnnotationPresent(UriPatternMapping.class)) {
        //addPatternMapping(handler, methods[i]);
      } else if (methods[i].isAnnotationPresent(UriVariableMapping.class)) {
        //addVariablePatternMapping(handler, methods[i]);
      }
    }
  }

  public void addDirectMapping(final HttpRequestHandler handler, final Method method) {
    final UriMapping uriMapping = method.getAnnotation(UriMapping.class);
    directRoutes.put(uriMapping.method() + uriMapping.uri(), new HttpRoute(handler, method));
  }

  public void addPatternMapping(final HttpRequestHandler handler, final Method method) {
    final UriPatternMapping uriMapping = method.getAnnotation(UriPatternMapping.class);
    addPatternMapping(uriMapping.method(), Pattern.compile(uriMapping.uri()), new HttpRoute(handler, method));
  }

  private static final Pattern URI_VARIABLE_MAPPING_PATTERN = Pattern.compile("\\{(.*?)\\}");
  public void addVariablePatternMapping(final HttpRequestHandler handler, final Method method) {
    final UriVariableMapping uriMapping = method.getAnnotation(UriVariableMapping.class);
    final Matcher m = URI_VARIABLE_MAPPING_PATTERN.matcher(uriMapping.uri());
    final StringBuilder uriPattern = new StringBuilder(uriMapping.uri().length());
    while (m.find()) {
      final String groupName = m.group(1);
      m.appendReplacement(uriPattern, "(?<" + groupName + ">[^/]*)");
    }
    m.appendTail(uriPattern);
    addPatternMapping(uriMapping.method(), Pattern.compile(uriPattern.toString()), new HttpRoute(handler, method));
  }

  private void addPatternMapping(final String method, final Pattern uri, final HttpRoute route) {
  }

  public static final class HttpRoute {
    private final HttpRequestHandler handler;
    private final Method method;

    private HttpRoute(final HttpRequestHandler handler, final Method method) {
      this.handler = handler;
      this.method = method;
    }

    public Object call(final ChannelHandlerContext ctx, final FullHttpRequest request)
        throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
      return method.invoke(handler, ctx, request);
    }
  }
}
