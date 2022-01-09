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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tech.dnaco.logging.Logger;
import tech.dnaco.net.http.HttpHandler.HttpMethod;
import tech.dnaco.net.http.HttpHandler.NoHttpTraceDump;
import tech.dnaco.net.http.HttpHandler.UriMapping;
import tech.dnaco.net.http.HttpHandler.UriPatternMapping;
import tech.dnaco.net.http.HttpHandler.UriPrefix;
import tech.dnaco.net.http.HttpHandler.UriVariableMapping;
import tech.dnaco.net.util.UriUtil;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.BitUtil;

public class HttpRouters {
  private HttpRouters() {
    // no-op
  }

  private static int hash(final String uri) {
    int h;
    return (h = uri.hashCode()) ^ (h >>> 16);
  }

  public static int httpMethodMask(final String method) {
    return httpMethodMask(HttpMethod.valueOf(method));
  }

  public static int httpMethodMask(final HttpMethod method) {
    return 1 << method.ordinal();
  }

  // =====================================================================================
  //  URI Router Builder
  // =====================================================================================
  public static final class UriRoutesBuilder {
    private final HashSet<HttpHandler> handlers = new HashSet<>();
    private final ArrayList<UriRoute> variableUri = new ArrayList<>();
    private final ArrayList<UriRoute> patternUri = new ArrayList<>();
    private final ArrayList<UriRoute> staticUri = new ArrayList<>();
    private final HashMap<String, String> aliases = new HashMap<>();
    private final ArrayList<StaticFileUriRoute> staticFilesUri = new ArrayList<>();

    public void addHandler(final HttpHandler handler) {
      final Method[] methods = handler.getClass().getMethods();
      for (int i = 0, n = methods.length; i < n; ++i) {
        final Method m = methods[i];
        if (m.isAnnotationPresent(UriMapping.class)) {
          addStaticMapping(handler, m);
        } else if (m.isAnnotationPresent(UriVariableMapping.class)) {
          addVariableMapping(handler, m);
        } else if (m.isAnnotationPresent(UriPatternMapping.class)) {
          addPatternMapping(handler, m);
        }
      }
      handlers.add(handler);
    }

    public void addStaticMapping(final HttpHandler handler, final Method method) {
      final boolean noHttpTraceDump = method.isAnnotationPresent(NoHttpTraceDump.class);
      final UriMapping uriMapping = method.getAnnotation(UriMapping.class);
      final HttpMethod[] httpMethods = uriMapping.method();

      final String uri = getAbsoluteUri(handler, uriMapping.uri());
      this.staticUri.add(new UriRoute(httpMethods, uri, handler, method));
      for (int i = 0; i < httpMethods.length; ++i) {
        Logger.debug("Add static HTTP URI Mapping {} {}", httpMethods[i], uri);
        if (noHttpTraceDump) {
          //HttpRecorder.INSTANCE.addToExcludeUrl(httpMethods[i], uri);
        }
      }
    }

    public void addVariableMapping(final HttpHandler handler, final Method method) {
      final boolean noHttpTraceDump = method.isAnnotationPresent(NoHttpTraceDump.class);
      final UriVariableMapping uriMapping = method.getAnnotation(UriVariableMapping.class);
      final HttpMethod[] httpMethods = uriMapping.method();

      final String uri = getAbsoluteUri(handler, uriMapping.uri());
      this.variableUri.add(new UriRoute(httpMethods, uri, handler, method));
      for (int i = 0; i < httpMethods.length; ++i) {
        Logger.debug("Add variable HTTP URI Mapping {} {}", httpMethods[i], uri);
        if (noHttpTraceDump) {
          //HttpRecorder.INSTANCE.addToExcludeUrl(httpMethods[i], uri);
        }
      }
    }

    public void addPatternMapping(final HttpHandler handler, final Method method) {
      final boolean noHttpTraceDump = method.isAnnotationPresent(NoHttpTraceDump.class);
      final UriPatternMapping uriMapping = method.getAnnotation(UriPatternMapping.class);
      final HttpMethod[] httpMethods = uriMapping.method();

      final String uri = getAbsoluteUri(handler, uriMapping.uri());
      this.patternUri.add(new UriRoute(httpMethods, uri, handler, method));
      for (int i = 0; i < httpMethods.length; ++i) {
        Logger.debug("Add pattern HTTP URI Mapping {} {}", httpMethods[i], uri);
        if (noHttpTraceDump) {
          //HttpRecorder.INSTANCE.addToExcludeUrl(httpMethods[i], uri);
        }
      }
    }

    public void addStaticFileHandler(final String uriPrefix, final File staticFileDir) {
      addStaticFileHandler(uriPrefix, staticFileDir, HttpMethod.GET);
    }

    public void addStaticFileHandler(final String uriPrefix, final File staticFileDir, final HttpMethod... httpMethods) {
      if (httpMethods.length == 0) {
        throw new IllegalArgumentException("expected at least one http method");
      }

      final String uri = uriPrefix + (uriPrefix.endsWith("/") ? "(.*)" : "/(.*)");
      this.staticFilesUri.add(new StaticFileUriRoute(httpMethods, uri, staticFileDir));
    }

    public void addAlias(final String alias, final String uriPrefix) {
      this.aliases.put(alias, uriPrefix);
    }

    private String getAbsoluteUri(final HttpHandler handler, final String path) {
      final UriPrefix handlerPrefixAnnotation = handler.getClass().getAnnotation(UriPrefix.class);
      final String handlerPrefix = (handlerPrefixAnnotation != null) ? handlerPrefixAnnotation.value() : null;
      return UriUtil.join(handlerPrefix, path);
    }

    public ArrayList<UriRoute> getVariableUri() {
      return getUrisAndApplyAliases(variableUri);
    }

    public ArrayList<UriRoute> getPatternUri() {
      return getUrisAndApplyAliases(patternUri);
    }

    public ArrayList<UriRoute> getStaticUri() {
      return getUrisAndApplyAliases(staticUri);
    }

    public ArrayList<StaticFileUriRoute> getStaticFilesUri() {
      return staticFilesUri;
    }

    public Set<HttpHandler> getHandlers() {
      return handlers;
    }

    private ArrayList<UriRoute> getUrisAndApplyAliases(final ArrayList<UriRoute> routes) {
      if (aliases.isEmpty()) return routes;

      final ArrayList<UriRoute> routesWithAliases = new ArrayList<>(routes);
      for (final UriRoute route: routes) {
        final String path = route.getUri();

        // TODO: improve with a sorted map and a reverse floor lookup
        String bestAlias = null;
        long bestAliasLen = 0;
        for (final Entry<String, String> aliasEntry: aliases.entrySet()) {
          if (aliasEntry.getKey().length() > bestAliasLen && path.startsWith(aliasEntry.getKey())) {
            bestAlias = aliasEntry.getValue() + path.substring(aliasEntry.getKey().length());
            bestAliasLen = aliasEntry.getKey().length();
          }
        }

        if (StringUtil.isNotEmpty(bestAlias)) {
          routesWithAliases.add(route.newWithAlias(bestAlias));
        }
      }
      return routesWithAliases;
    }
  }

  private static abstract class AbstractUriRoute implements Comparable<AbstractUriRoute> {
    private final int httpMethods;
    private final String uri;

    protected AbstractUriRoute(final HttpMethod[] methods, final String uri) {
      this(getMask(methods), uri);
    }

    private AbstractUriRoute(final int httpMethods, final String uri) {
      this.httpMethods = httpMethods;
      this.uri = uri;
    }

    protected static int getMask(final HttpMethod[] methods) {
      int mask = 0;
      for (int i = 0, n = methods.length; i < n; ++i) {
        mask |= 1 << methods[i].ordinal();
      }
      return mask;
    }

    public int getHttpMethods() {
      return httpMethods;
    }

    public String getUri() {
      return uri;
    }

    @Override
    public int compareTo(final AbstractUriRoute other) {
      return uri.compareTo(other.uri);
    }
  }

  public static final class UriRoute extends AbstractUriRoute {
    private final HttpHandler handler;
    private final Method method;

    public UriRoute(final HttpMethod[] methods, final String uri, final HttpHandler handler, final Method method) {
      super(methods, uri);
      this.handler = handler;
      this.method = method;
    }

    private UriRoute(final int httpMethods, final String uri, final HttpHandler handler, final Method method) {
      super(httpMethods, uri);
      this.handler = handler;
      this.method = method;
    }

    private UriRoute newWithAlias(final String alias) {
      return new UriRoute(getHttpMethods(), alias, handler, method);
    }

    public HttpHandler getHandler() {
      return handler;
    }

    public Method getMethod() {
      return method;
    }

    @Override
    public String toString() {
      return "UriRoute [httpMethods=" + getHttpMethods() + ", uri=" + getUri() + ", handler=" + handler + ", method=" + method + "]";
    }
  }

  public static final class StaticFileUriRoute extends AbstractUriRoute {
    private final File staticFileDir;

    protected StaticFileUriRoute(final HttpMethod[] methods, final String uri, final File staticFileDir) {
      super(methods, uri);
      this.staticFileDir = staticFileDir;
    }

    public File getStaticFileDir() {
      return staticFileDir;
    }

    @Override
    public String toString() {
      return "StaticFileUriRoute [httpMethods=" + getHttpMethods() + ", uri=" + getUri() + ", staticFileDir=" + staticFileDir + "]";
    }
  }

  // =====================================================================================
  //  Static URI Router
  // =====================================================================================
  public static final class UriStaticRouter<T> {
    private final UriStaticRouteNode[] buckets;

    public UriStaticRouter(final List<UriRoute> routes, final Function<UriRoute, T> builder) {
      this.buckets = new UriStaticRouteNode[BitUtil.nextPow2(routes.size()) << 1];
      int collisions = 0;
      for (final UriRoute route: routes) {
        final T handler = builder.apply(route);
        final int hash = hash(route.getUri());
        final int index = hash & (buckets.length - 1);
        collisions += (buckets[index] != null) ? 1 : 0;
        buckets[index] = new UriStaticRouteNode(hash, route.getHttpMethods(), route.getUri(), buckets[index], handler);
      }
      Logger.trace("static router size {} routes {} collisions {}", buckets.length, routes.size(), collisions);
    }

    public T get(final int methodMask, final String uri) {
      final int hash = hash(uri);
      final int index = hash & (buckets.length - 1);
      for (UriStaticRouteNode node = buckets[index]; node != null; node = node.next) {
        if (node.hash == hash && (node.methods & methodMask) != 0 && node.uri.equals(uri)) {
          return node.getHandler();
        }
      }
      return null;
    }
  }

  private static final class UriStaticRouteNode {
    private final int hash;
    private final int methods;
    private final String uri;
    private final Object handler;
    private final UriStaticRouteNode next;

    private UriStaticRouteNode(final int hash, final int methods, final String uri,
        final UriStaticRouteNode next, final Object handler) {
      this.hash = hash;
      this.methods = methods;
      this.uri = uri;
      this.next = next;
      this.handler = handler;
    }

    @SuppressWarnings("unchecked")
    private <T> T getHandler() {
      return (T)handler;
    }
  }

  // =====================================================================================
  //  Variable URI Router
  // =====================================================================================
  public static final class UriVariableRouter<T> {
    private final ThreadLocal<UriVariableHandler<T>> localResult = ThreadLocal.withInitial(UriVariableHandler<T>::new);

    private UriVariableNodes[] nodes = new UriVariableNodes[8];

    public UriVariableRouter(final ArrayList<UriRoute> routes, final Function<UriRoute, T> builder) {
      Collections.sort(routes);
      for (final UriRoute route: routes) {
        final UriVariableParts uriParts = new UriVariableParts(route.getUri());
        final int parts = uriParts.getCount();
        if (parts >= nodes.length) nodes = Arrays.copyOf(nodes, parts + 1);
        if (nodes[parts] == null) nodes[parts] = new UriVariableNodes();
        nodes[parts].add(route.getHttpMethods(), uriParts, builder.apply(route));
      }

      Logger.trace("variable router parts {} routes {}", nodes.length, routes.size());
    }

    public UriVariableHandler<T> get(final int methodMask, final String uri) {
      final UriParts uriParts = localUriParts.get();
      final int parts = uriParts.build(uri);
      if (parts >= nodes.length) return null;

      final UriVariableNodes node = nodes[parts];
      if (node == null) return null;

      return node.get(methodMask, uriParts, localResult.get());
    }

    private static final class UriVariableNodes {
      private int[] methods = new int[0];
      private UriVariableParts[] uris = new UriVariableParts[0];
      private Object[] handlers = new Object[0];

      public void add(final int httpMethods, final UriVariableParts uriParts, final Object handler) {
        final int index = methods.length;
        grow();

        // TODO: optimize me. sort and lookup on prefix
        methods[index] = httpMethods;
        uris[index] = uriParts;
        handlers[index] = handler;
      }

      public <T> UriVariableHandler<T> get(final int methodMask, final UriParts uriParts,
          final UriVariableHandler<T> result) {
        for (int i = 0, n = methods.length; i < n; ++i) {
          if ((methods[i] & methodMask) != 0 && uris[i].matches(uriParts)) {
            uris[i].extractVariables(uriParts, result.getVariables());
            result.setHandler(handlers[i]);
            return result;
          }
        }
        return null;
      }

      private void grow() {
        methods = Arrays.copyOf(methods, methods.length + 1);
        uris = Arrays.copyOf(uris, uris.length + 1);
        handlers = Arrays.copyOf(handlers, handlers.length + 1);
      }
    }
  }

  public static final class UriVariableHandler<T> {
    private final Map<String, String> variables = new HashMap<>();
    private T handler;

    @SuppressWarnings("unchecked")
    private void setHandler(final Object handler) {
      this.handler = (T) handler;
    }

    public T getHandler() {
      return handler;
    }

    public Map<String, String> getVariables() {
      return variables;
    }

    @Override
    public String toString() {
      return "UriVariableHandler[" + handler + ", vars=" + variables + "]";
    }
  }

  // =====================================================================================
  //  Pattern URI Router
  // =====================================================================================
  public static final class UriPatternRouter<T> {
    private final ThreadLocal<UriPatternHandler<T>> localResult = ThreadLocal.withInitial(UriPatternHandler<T>::new);

    private final int[] methods;
    private final Pattern[] uris;
    private final Object[] handlers;

    public <TRoute extends AbstractUriRoute> UriPatternRouter(final ArrayList<TRoute> routes, final Function<TRoute, T> builder) {
      this.methods = new int[routes.size()];
      this.uris = new Pattern[routes.size()];
      this.handlers = new Object[routes.size()];

      for (int i = 0, n = routes.size(); i < n; ++i) {
        // TODO: optimize me. sort and lookup on prefix
        final TRoute route = routes.get(i);
        methods[i] = route.getHttpMethods();
        uris[i] = compile(route.getUri());
        handlers[i] = builder.apply(route);
      }
    }

    private static final Pattern URI_VARIABLE_MAPPING_PATTERN = Pattern.compile("\\{(.*?)\\}");
    private static Pattern compile(final String uri) {
      final Matcher m = URI_VARIABLE_MAPPING_PATTERN.matcher(uri);
      final StringBuilder uriPattern = new StringBuilder(uri.length() + 32);

      boolean hasVariable = false;
      while (m.find()) {
        final String groupName = m.group(1);
        m.appendReplacement(uriPattern, "(?<" + groupName + ">[^/]*)");
        hasVariable = true;
      }
      m.appendTail(uriPattern);

      if (!hasVariable && uri.indexOf('(') < 0) {
        throw new UnsupportedOperationException("@UriPatternMapping is slow, do not use it if not needed. Use @UriMapping for " + uri);
      }

      return Pattern.compile(uriPattern.toString());
    }

    public UriPatternHandler<T> get(final String method, final String uri) {
      return get(httpMethodMask(method), uri);
    }

    public UriPatternHandler<T> get(final int methodMask, final String uri) {
      for (int i = 0, n = methods.length; i < n; ++i) {
        if ((methods[i] & methodMask) == 0) continue;

        final Matcher matcher = uris[i].matcher(uri);
        if (matcher.matches()) {
          final UriPatternHandler<T> result = localResult.get();

          result.setMatcher(matcher);
          result.setHandler(handlers[i]);
          return result;
        }
      }
      return null;
    }
  }

  public static final class UriPatternHandler<T> {
    private Matcher matcher;
    private T handler;

    @SuppressWarnings("unchecked")
    private void setHandler(final Object handler) {
      this.handler = (T) handler;
    }

    private void setMatcher(final Matcher matcher) {
      this.matcher = matcher;
    }

    public T getHandler() {
      return handler;
    }

    public Matcher getMatcher() {
      return matcher;
    }
  }

  // =====================================================================================
  //  URI Parts
  // =====================================================================================
  private static final ThreadLocal<UriParts> localUriParts = ThreadLocal.withInitial(UriParts::new);
  static final class UriParts {
    private final int[] partOffset = new int[64];
    private final int[] partHash = new int[63];
    private String uri;
    private int count;

    public UriParts() {
      // no-op
    }

    public UriParts(final String uri) {
      build(uri);
    }

    public int build(final String uri) {
      final int uriLength = uri.length();
      this.uri = uri;

      // extract parts
      int partIndex = 0;
      partHash[0] = 0;
      for (int i = 1; i < uriLength; ++i) {
        final char c = uri.charAt(i);
        if (c != '/') {
          partHash[partIndex] = 31 * partHash[partIndex] + (c & 0xff);
        } else {
          partOffset[partIndex++] = i;
          partHash[partIndex] = 0;
        }
      }
      partOffset[partIndex] = uriLength;
      partOffset[partIndex + 1] = 0;
      count = partIndex + 1;
      return count;
    }

    public int getCount() {
      return count;
    }

    @Override
    public String toString() {
      return uriPartsToString(getClass().getSimpleName(), uri, partOffset);
    }
  }

  // =====================================================================================
  //  URI Variable Parts
  // =====================================================================================
  static final class UriVariableParts {
    private final String[] variableName;
    private final int[] partOffset;
    private final int[] partHash;
    private final String uri;

    public UriVariableParts(final String uri) {
      this.uri = uri;

      // count parts
      final int length = uri.length();
      int parts = 0;
      for (int i = 0; i < length; ++i) {
        parts += (uri.charAt(i) == '/') ? 1 : 0;
      }

      // alloc members
      this.variableName = new String[parts];
      this.partOffset = new int[parts + 1];
      this.partHash = new int[parts];

      // extract
      int partIndex = 0;
      for (int i = 1; i < length; ++i) {
        final char c = uri.charAt(i);
        if (c != '/') {
          partHash[partIndex] = 31 * partHash[partIndex] + (c & 0xff);
        } else {
          partOffset[partIndex++] = i;
        }
      }
      partOffset[partIndex] = length;

      // check for regex
      if (uri.indexOf('(') >= 0) {
        throw new UnsupportedOperationException("uri variables do not support patterns. use @UriPatternMapping for " + uri);
      }

      // extract variables
      int lastOffset = 1;
      for (int i = 0; i < parts; ++i) {
        final int varIndex = uri.indexOf('{', lastOffset);
        if (varIndex > 0 && varIndex < partOffset[i]) {
          final int endVarIndex = uri.indexOf('}', lastOffset + 1) + 1;
          if (varIndex != lastOffset || endVarIndex != partOffset[i])  {
            throw new UnsupportedOperationException("uri variables are expected to be full uri parts /{var}/ got " + uri);
          }
          this.variableName[i] = uri.substring(lastOffset + 1, partOffset[i] - 1);
        }
        lastOffset = partOffset[i] + 1;
      }
    }

    public int getCount() {
      return variableName.length;
    }

    public boolean matches(final UriParts other) {
      final String otherUri = other.uri;
      final int[] otherHash = other.partHash;
      final int[] otherOffset = other.partOffset;

      //System.out.println(" -> " + otherUri + " -> " + uri);
      //System.out.println(" -> " + Arrays.toString(partHash));
      //System.out.println(" -> " + Arrays.toString(otherHash));
      //System.out.println(" -> " + Arrays.toString(partOffset));
      //System.out.println(" -> " + Arrays.toString(otherOffset));

      int lastOffset = 1;
      int cmpLastOffset = 1;
      int index = 0;
      do {
        //System.out.println(" --> " + index + " -> '" + uri.substring(lastOffset, partOffset[index]) + "' -> '" + otherUri.substring(cmpLastOffset, otherOffset[index]) + "'");
        //System.out.println(" --> " + index + " -> " + partHash[index] + " -> " + otherHash[index] + " -> " + variableName[index]);

        if (variableName[index] == null) {
          if (partHash[index] != otherHash[index]) return false;

          if (!StringUtil.equals(uri, lastOffset, partOffset[index] - lastOffset, otherUri, cmpLastOffset, otherOffset[index] - cmpLastOffset)) {
            //System.out.println("-----|NOT MATCH|-----: " + StringUtil.equals(uri, lastOffset, partOffset[index], otherUri, cmpLastOffset, otherOffset[index]));
            return false;
          }
        }

        lastOffset = partOffset[index] + 1;
        cmpLastOffset = otherOffset[index] + 1;
      } while (partOffset[++index] > 0);
      return true;
    }

    public void extractVariables(final UriParts parts, final Map<String, String> variables) {
      final int[] offsets = parts.partOffset;
      int index = 0;
      int lastOffset = 1;
      do {
        final String varName = variableName[index];
        if (varName != null) {
          final String varValue = parts.uri.substring(lastOffset, offsets[index]);
          variables.put(varName, varValue);
        }
        lastOffset = offsets[index] + 1;
      } while (partOffset[++index] > 0);
    }

    @Override
    public String toString() {
      return uriPartsToString(getClass().getSimpleName(), uri, partOffset);
    }
  }

  private static String uriPartsToString(final String name, final String uri, final int[] offsets) {
    final StringBuilder builder = new StringBuilder(64);
    builder.append(name).append(" [");
    int lastOffset = 1;
    int index = 0;
    do {
      if (index > 0) builder.append(", ");
      builder.append("'").append(uri, lastOffset, offsets[index]).append("'");
      lastOffset = offsets[index] + 1;
    } while (offsets[++index] > 0);
    builder.append("]");
    return builder.toString();
  }
}
