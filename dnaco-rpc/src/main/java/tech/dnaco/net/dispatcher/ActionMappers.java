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

package tech.dnaco.net.dispatcher;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.logging.Logger;

public class ActionMappers {
  private final Map<Class<? extends Annotation>, ActionParserFactory> parserFactories = new HashMap<>();

  public boolean addMapper(final Class<? extends Annotation> annotationType, final ActionParserFactory factory) {
    final ActionParserFactory oldFactory = parserFactories.put(annotationType, factory);
    if (oldFactory != null) {
      Logger.warn("action parser factory {} for annotation {} is replacing {}", factory, annotationType, oldFactory);
      return false;
    }
    return true;
  }

  public ActionParser newParser(final Annotation action) {
    final ActionParserFactory factory = parserFactories.get(action.annotationType());
    if (factory == null) return null;
    return factory.newParser(action);
  }

  public void newParsers(final Collection<ActionParser> parsers, final Annotation[] actions) {
    if (ArrayUtil.isEmpty(actions)) return;

    for (final Annotation action: actions) {
      final ActionParser parser = newParser(action);
      if (parser == null) continue;
      parsers.add(parser);
    }
  }

  protected ActionParser[] parseActions(final Method method) {
    final ArrayList<ActionParser> parsers = new ArrayList<>();
    newParsers(parsers, method.getDeclaringClass().getAnnotations());
    newParsers(parsers, method.getAnnotations());
    return parsers.toArray(new ActionParser[0]);
  }
}
