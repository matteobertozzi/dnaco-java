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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.dispatcher.Actions.AsyncResult;
import tech.dnaco.logging.Logger;

public class MethodInvoker {
  private final Object handler;
  private final Method method;
  private final ParamParser[] paramParsers;
  private final ActionParser[] actionParsers;
  private final boolean asyncResult;
  private final boolean voidResult;

  public MethodInvoker(final Object handler, final Method method,
      final ActionParser[] actionParsers, final ParamParser[] paramParsers) {
    this.handler = handler;
    this.method = method;
    this.paramParsers = paramParsers;
    this.actionParsers = actionParsers;
    this.asyncResult = hasAnnotation(AsyncResult.class);
    this.voidResult = hasVoidResult(method);
    Logger.trace("{handler} {method} {asyncResult} {voidResult} {actions} {params}",
      handler, method, voidResult, actionParsers, paramParsers);
  }

  private static boolean hasVoidResult(final Method method) {
    final Class<?> returnType = method.getReturnType();
    return returnType == void.class || returnType == Void.class;
  }

  public boolean hasAnnotation(final Class<? extends Annotation> annotationType) {
    return ArrayUtil.isNotEmpty(method.getDeclaringClass().getAnnotationsByType(annotationType))
        || ArrayUtil.isNotEmpty(method.getAnnotationsByType(annotationType));
  }

  public boolean hasAsyncResult() {
    return asyncResult;
  }

  public boolean hasVoidResult() {
    return voidResult;
  }

  public Object invoke(final CallContext context, final Object message) throws Throwable {
    // call action-parsers (before param-parse)
    for (int i = 0; i < actionParsers.length; ++i) {
      if (actionParsers[i].beforeParamParse(context, message)) {
        return callActionAfterExecute(context, message, null, i);
      }
    }

    // call param-parsers
    final Object[] params = new Object[paramParsers.length];
    for (int i = 0; i < params.length; ++i) {
      params[i] = paramParsers[i].parse(context, message);
    }

    // call action-parsers (before execute)
    for (int i = 0; i < actionParsers.length; ++i) {
      if (actionParsers[i].beforeExecute(context, method, params, message)) {
        return callActionAfterExecute(context, message, null, i);
      }
    }

    // execute
    try {
      final Object result = method.invoke(handler, params);

      // call action-parsers (after execute)
      return callActionAfterExecute(context, message, result, actionParsers.length);
    } catch (final InvocationTargetException e) {
      throw e.getCause();
    } catch (final IllegalArgumentException e) {
      Logger.error(e, "failed to call {handler} {method} {params}", handler, method.getName(), params);
      throw e;
    }
  }

  private Object callActionAfterExecute(final CallContext context, final Object message,
      Object result, final int count) throws Exception {
    for (int i = 0; i < count; ++i) {
      result = actionParsers[i].afterExecute(context, message, result);
    }
    return result;
  }

  @Override
  public String toString() {
    return "MethodInvoker [handler=" + handler + ", method=" + method + "]";
  }
}
