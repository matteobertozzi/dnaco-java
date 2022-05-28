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
package tech.dnaco.dispatcher.message;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;

import tech.dnaco.data.DataFormat;

public interface MessageHandler {
  enum UriMethod {
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
    UriMethod[] method() default { UriMethod.GET };
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  @interface UriPatternMapping {
    String uri();
    UriMethod[] method() default { UriMethod.GET };
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  @interface UriVariableMapping {
    String uri();
    UriMethod[] method() default { UriMethod.GET };
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface UriVariable {
    String value() default "";
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface UriPattern {
    int value() default -1;
  }

  // ===========================================================================
  //  Method params related
  // ===========================================================================
  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface QueryParam {
    String value() default "";
    String name() default "";
    String defaultValue() default "";
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface HeaderValue {
    String value() default "";
    String name() default "";
    String defaultValue() default "";
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

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface Context {
  }

  // ===========================================================================
  //  Actions
  // ===========================================================================
  @Retention(RUNTIME)
  @Target(METHOD)
  @interface NoTraceDump {
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  @interface Task {
  }

  interface MessageData {
    String method();
    String path();

    List<String> queryParamAsList(String name);
    List<String> metadataValueAsList(String name);
    String getMetadata(String key, String defaultValue);

    <T> T convertBody(DataFormat format, Class<T> classOfT);
  }
}
