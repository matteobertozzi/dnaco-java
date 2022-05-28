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

package tech.dnaco.dispatcher;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class MessageDispatcher {
  private final ActionMappers actionMappers = new ActionMappers();
  private final ParamMappers paramMappers = new ParamMappers();

  public boolean addActionMapper(final Class<? extends Annotation> annotationType, final ActionParserFactory factory) {
    return actionMappers.addMapper(annotationType, factory);
  }

  public boolean addParamDefaultMapper(final ParamParserFactory factory) {
    return paramMappers.addDefaultMapper(factory);
  }

  public boolean addParamTypeMapper(final Class<?> type, final ParamParserFactory factory) {
    return paramMappers.addTypeMapper(type, factory);
  }

  public boolean addParamAnnotationMapper(final Class<? extends Annotation> annotationType, final ParamParserFactory factory) {
    return paramMappers.addAnnotationMapper(annotationType, factory);
  }

  protected ActionParser[] parseActions(final Method method) {
    return actionMappers.parseActions(method);
  }

  protected ParamParser[] parseParams(final Method method) {
    return paramMappers.parseParams(method);
  }

  public MethodInvoker newMethodInvoker(final Object handler, final Method method) {
    final ActionParser[] actionParsers = parseActions(method);
    final ParamParser[] paramParsers = parseParams(method);
    return new MethodInvoker(handler, method, actionParsers, paramParsers);
  }
}
