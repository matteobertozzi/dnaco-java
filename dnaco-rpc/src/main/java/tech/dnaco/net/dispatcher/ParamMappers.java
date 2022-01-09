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
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.logging.Logger;

public class ParamMappers {
  private final Map<Class<? extends Annotation>, ParamParserFactory> parserFactories = new HashMap<>();
  private final Map<Class<?>, ParamParserFactory> typeFactories = new HashMap<>();
  private ParamParserFactory defaultFactory;

  public boolean addDefaultMapper(final ParamParserFactory factory) {
    final ParamParserFactory oldFactory = defaultFactory;
    if (oldFactory != null) {
      Logger.warn("param parser factory {} for default type is replacing {}", factory, oldFactory);
      return false;
    }
    this.defaultFactory = factory;
    return true;
  }

  public boolean addTypeMapper(final Class<?> type, final ParamParserFactory factory) {
    final ParamParserFactory oldFactory = typeFactories.put(type, factory);
    if (oldFactory != null) {
      Logger.warn("param parser factory {} for type {} is replacing {}", factory, type, oldFactory);
      return false;
    }
    return true;
  }

  public boolean addAnnotationMapper(final Class<? extends Annotation> annotationType, final ParamParserFactory factory) {
    final ParamParserFactory oldFactory = parserFactories.put(annotationType, factory);
    if (oldFactory != null) {
      Logger.warn("param parser factory {} for annotation {} is replacing {}", factory, annotationType, oldFactory);
      return false;
    }
    return true;
  }

  public ParamParser newParser(final Parameter param) {
    System.out.println("NEW PARSER " + param);
    // annotation-based param parser
    final Annotation[] annotations = param.getAnnotations();
    if (ArrayUtil.isNotEmpty(annotations)) {
      final ArrayList<ParamParser> parsers = new ArrayList<>();
      for (final Annotation annotation: annotations) {
        System.out.println(" - Annotation: " + annotation);
        final ParamParserFactory factory = parserFactories.get(annotation.annotationType());
        System.out.println(" - Factory: " + factory);
        if (factory == null) continue;

        final ParamParser parser = factory.newParser(param, annotation);
        System.out.println(" - ParamParser: " + parser);
        if (parser != null) parsers.add(parser);
      }

      System.out.println(" - PARAM PARSER: " + parsers.size() + " " + param);
      if (!parsers.isEmpty()) {
        if (parsers.size() > 1) {
          return new ChainedParamParser(parsers);
        }
        return parsers.get(0);
      }
    }

    // type-based param parser
    final ParamParserFactory factory = typeFactories.get(param.getType());
    if (factory != null) return factory.newParser(param, null);

    // default param parser
    return defaultFactory != null ? defaultFactory.newParser(param, null) : null;
  }

  public ParamParser[] parseParams(final Method method) {
    final Parameter[] params = method.getParameters();
    if (ArrayUtil.isEmpty(params)) return new ParamParser[0];

    final ParamParser[] parsers = new ParamParser[params.length];
    for (int i = 0; i < params.length; ++i) {
      parsers[i] = newParser(params[i]);
    }
    return parsers;
  }

  private static final class ChainedParamParser implements ParamParser {
    private final ParamParser[] parsers;

    private ChainedParamParser(final List<ParamParser> parsers) {
      this.parsers = parsers.toArray(new ParamParser[0]);
    }

    @Override
    public Object parse(final CallContext context, final Object message) throws Exception {
      Object result = message;
      for (int i = 0; i < parsers.length; ++i) {
        result = parsers[i].parse(context, result);
      }
      return result;
    }

    @Override
    public String toString() {
      return "ChainedParamParser " + Arrays.toString(parsers);
    }
  }
}
