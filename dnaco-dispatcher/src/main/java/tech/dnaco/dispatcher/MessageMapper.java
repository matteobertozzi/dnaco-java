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
import java.util.HashMap;
import java.util.function.Function;

import tech.dnaco.dispatcher.ParamMappers.ParamConverter;
import tech.dnaco.dispatcher.message.Message;
import tech.dnaco.dispatcher.message.MessageError;
import tech.dnaco.logging.Logger;

public class MessageMapper {
  private final HashMap<Class<? extends Throwable>, Function<Throwable, MessageError>> exceptionMappers = new HashMap<>(64);
  private final HashMap<Class<?>, Function<Object, Message>> typeToMessageMappers = new HashMap<>(64);
  private final ActionMappers actionMappers = new ActionMappers();
  private final ParamMappers paramMappers = new ParamMappers();

  public ParamConverter paramConverter() {
    return paramMappers.paramConverter();
  }

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

  @SuppressWarnings("unchecked")
  public <T extends Throwable> boolean addExceptionMapper(final Class<T> exceptionType, final Function<T, MessageError> mapper) {
    final Function<? extends Throwable, MessageError> oldMapper = exceptionMappers.put((Class<? extends Throwable>)exceptionType, (Function<Throwable, MessageError>) mapper);
    if (oldMapper != null) {
      Logger.warn("exception mapper {} for exception {} is replacing {}", mapper, exceptionType, oldMapper);
      return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  public <T> boolean addTypeMapper(final Class<T> classType, final Function<T, Message> mapper) {
    final Function<?, Message> oldMapper = typeToMessageMappers.put((Class<?>)classType, (Function<Object, Message>)mapper);
    if (oldMapper != null) {
      Logger.warn("type mapper {} for {} is replacing {}", mapper, classType, oldMapper);
      return false;
    }
    return true;
  }

  public final Message mapTypedResultToMessage(final Object result) {
    final Function<Object, Message> func = this.typeToMessageMappers.get(result.getClass());
    return func != null ? func.apply(result) : null;
  }

  protected ActionParser[] parseActions(final Method method) {
    return actionMappers.parseActions(method);
  }

  protected ParamParser[] parseParams(final Method method) {
    return paramMappers.parseParams(method);
  }

  public MessageError mapException(final Throwable exception) {
    Class<?> classOfException = exception.getClass();
    do {
      final Function<Throwable, MessageError> mapper = exceptionMappers.get(classOfException);
      if (mapper != null) return mapper.apply(exception);

      classOfException = classOfException.getSuperclass();
    } while (classOfException != Throwable.class && classOfException != null);
    return null;
  }

  public MethodInvoker newMethodInvoker(final Object handler, final Method method) {
    final ActionParser[] actionParsers = parseActions(method);
    final ParamParser[] paramParsers = parseParams(method);
    return new MethodInvoker(handler, method, actionParsers, paramParsers);
  }
}
