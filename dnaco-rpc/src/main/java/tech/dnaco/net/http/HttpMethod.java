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

import java.util.HashMap;

public final class HttpMethod {
  /**
   * The OPTIONS method represents a request for information about the communication options
   * available on the request/response chain identified by the Request-URI. This method allows
   * the client to determine the options and/or requirements associated with a resource, or the
   * capabilities of a server, without implying a resource action or initiating a resource
   * retrieval.
   */
  public static final String OPTIONS = "OPTIONS";

  /**
   * The GET method means retrieve whatever information (in the form of an entity) is identified
   * by the Request-URI.  If the Request-URI refers to a data-producing process, it is the
   * produced data which shall be returned as the entity in the response and not the source text
   * of the process, unless that text happens to be the output of the process.
   */
  public static final String GET = "GET";

  /**
   * The HEAD method is identical to GET except that the server MUST NOT return a message-body
   * in the response.
   */
  public static final String HEAD = "HEAD";

  /**
   * The POST method is used to request that the origin server accept the entity enclosed in the
   * request as a new subordinate of the resource identified by the Request-URI in the
   * Request-Line.
   */
  public static final String POST = "POST";

  /**
   * The PUT method requests that the enclosed entity be stored under the supplied Request-URI.
   */
  public static final String PUT = "PUT";

  /**
   * The PATCH method requests that a set of changes described in the
   * request entity be applied to the resource identified by the Request-URI.
   */
  public static final String PATCH = "PATCH";

  /**
   * The DELETE method requests that the origin server delete the resource identified by the
   * Request-URI.
   */
  public static final String DELETE = "DELETE";

  /**
   * The TRACE method is used to invoke a remote, application-layer loop- back of the request
   * message.
   */
  public static final String TRACE = "TRACE";

  /**
   * This specification reserves the method name CONNECT for use with a proxy that can dynamically
   * switch to being a tunnel
   */
  public static final String CONNECT = "CONNECT";

  private HttpMethod() {
    // no-op
  }

  private static final HashMap<String, String> METHODS = new HashMap<>(16, 1.0f);
  static {
    METHODS.put(OPTIONS, OPTIONS);
    METHODS.put(GET, GET);
    METHODS.put(HEAD, HEAD);
    METHODS.put(POST, POST);
    METHODS.put(PUT, PUT);
    METHODS.put(PATCH, PATCH);
    METHODS.put(DELETE, DELETE);
    METHODS.put(TRACE, TRACE);
    METHODS.put(CONNECT, CONNECT);
  }

  public static boolean isValid(final String method) {
    return METHODS.containsKey(method);
  }

  public static String valueOf(final String method) {
    final String m = METHODS.get(method);
    if (m == null) {
      throw new IllegalArgumentException("invalid HTTP Method: " + method);
    }
    return m;
  }

  public static int getOrdinal(final String method) {
    switch (method) {
      case OPTIONS: return 1;
      case GET:     return 2;
      case HEAD:    return 3;
      case POST:    return 4;
      case PUT:     return 5;
      case PATCH:   return 6;
      case DELETE:  return 7;
      case TRACE:   return 8;
      case CONNECT: return 9;
    }
    throw new UnsupportedOperationException();
  }

  public static int getMask(final String method) {
    return 1 << getOrdinal(method);
  }

  public static int getMask(final String[] methods) {
    int mask = 0;
    for (int i = 0, n = methods.length; i < n; ++i) {
      mask |= getMask(methods[i]);
    }
    return mask;
  }
}
