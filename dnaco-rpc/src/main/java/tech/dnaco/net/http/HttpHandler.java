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

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

public interface HttpHandler {
  enum HttpMethod {
    // The OPTIONS method represents a request for information about the communication options
    // available on the request/response chain identified by the Request-URI. This method allows
    // the client to determine the options and/or requirements associated with a resource, or the
    // capabilities of a server, without implying a resource action or initiating a resource retrieval.
    OPTIONS,

    // The GET method means retrieve whatever information (in the form of an entity) is identified
    // by the Request-URI.  If the Request-URI refers to a data-producing process, it is the
    // produced data which shall be returned as the entity in the response and not the source text
    // of the process, unless that text happens to be the output of the process.
    GET,

    // The HEAD method is identical to GET except that the server MUST NOT return a message-body in the response.
    HEAD,

    // The POST method is used to request that the origin server accept the entity enclosed in the
    // request as a new subordinate of the resource identified by the Request-URI in the Request-Line.
    POST,

    // The PUT method requests that the enclosed entity be stored under the supplied Request-URI.
    PUT,

    // The PATCH method requests that a set of changes described in the
    // request entity be applied to the resource identified by the Request-URI.
    PATCH,

    // The DELETE method requests that the origin server delete the resource identified by the Request-URI.
    DELETE,

    // The TRACE method is used to invoke a remote, application-layer loop- back of the request message.
    TRACE,

    // This specification reserves the method name CONNECT for use with a proxy that can dynamically switch to being a tunnel
    CONNECT,
  }

  default void destroy() {}

  // ===========================================================================
  //  URI Mapping related
  // ===========================================================================
  @Retention(RUNTIME)
  @Target(TYPE)
  @interface UriPrefix {
    String value();
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  @interface UriMapping {
    String uri();
    HttpMethod[] method() default { HttpMethod.GET };
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  @interface UriPatternMapping {
    String uri();
    HttpMethod[] method() default { HttpMethod.GET };
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  @interface UriVariableMapping {
    String uri();
    HttpMethod[] method() default { HttpMethod.GET };
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface UriVariable {
    String value() default "";
    String name() default ""; // deprecated
  }

  // ===========================================================================
  //  Method params related
  // ===========================================================================
  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface QueryParam {
    String value() default "";
    String name() default ""; // deprecated
    String defaultValue() default "";
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface HeaderValue {
    String value() default "";
    String name() default ""; // deprecated
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface JsonBody {
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface CborBody {
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface XmlBody {
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface FormEncodedBody {
  }

  // ===========================================================================
  //  Actions
  // ===========================================================================
  @Retention(RUNTIME)
  @Target(METHOD)
  @interface NoHttpTraceDump {
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  @interface Task {
  }
}
