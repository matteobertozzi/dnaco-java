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

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.dispatcher.ParamMappers.ParamConverter;

public class TestMessageDispatcher {
  @Test
  public void testDispatcher() {
    final MessageMapper dispatcher = new MessageMapper();
    dispatcher.addActionMapper(TestActionA.class, TestActionParserA::new);
    dispatcher.addActionMapper(TestActionB.class, TestActionParserB::new);
    dispatcher.addParamAnnotationMapper(TestParamA.class, TestParamParserA::new);
    dispatcher.addParamAnnotationMapper(TestParamB.class, TestParamParserB::new);
    dispatcher.addParamAnnotationMapper(TestIntParam.class, TestIntParamParser::new);
    dispatcher.addParamDefaultMapper(TestDefaultParamParser::new);

    final TestHandler handler = new TestHandler();
    final Method[] methods = handler.getClass().getDeclaredMethods();
    Assertions.assertEquals(1, methods.length);

    final ActionParser[] actions = dispatcher.parseActions(methods[0]);
    final ParamParser[] params = dispatcher.parseParams(methods[0]);
    System.out.println(Arrays.toString(actions));
    System.out.println(Arrays.toString(params));
  }

  @TestActionA(name = "actionA")
  public static class TestHandler {
    @TestActionB(name = "actionB")
    public void testMethod(@TestParamA(name = "a") final String a,
        @TestParamB(name = "b") @TestIntParam final int b,
        final String c,
        final int d) {

    }
  }

  public static class TestActionParserA implements ActionParser {
    public TestActionParserA(final Annotation annotation) {
      // TODO
    }

    @Override
    public boolean beforeParamParse(final CallContext context, final Object message) throws Exception {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean beforeExecute(final CallContext context, final Method method, final Object[] params, final Object message) throws Exception {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Object afterExecute(final CallContext context, final Object message, final Object result) throws Exception {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public static class TestActionParserB implements ActionParser {
    public TestActionParserB(final Annotation annotation) {
      // TODO
    }

    @Override
    public boolean beforeParamParse(final CallContext context, final Object message) throws Exception {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean beforeExecute(final CallContext context, final Method method, final Object[] params, final Object message) throws Exception {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Object afterExecute(final CallContext context, final Object message, final Object result) throws Exception {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public static class TestParamParserA implements ParamParser {
    public TestParamParserA(final Parameter param, final Annotation annotation) {
      // TODO
    }

    @Override
    public Object parse(final CallContext context, final ParamConverter converter, final Object message) {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public static class TestParamParserB implements ParamParser {
    public TestParamParserB(final Parameter param, final Annotation annotation) {
      // TODO
    }

    @Override
    public Object parse(final CallContext context, final ParamConverter converter, final Object message) {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public static class TestIntParamParser implements ParamParser {
    public TestIntParamParser(final Parameter param, final Annotation annotation) {
      // TODO
    }

    @Override
    public Object parse(final CallContext context, final ParamConverter converter, final Object message) {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public static class TestDefaultParamParser implements ParamParser {
    public TestDefaultParamParser(final Parameter param, final Annotation annotation) {
      // TODO
    }

    @Override
    public Object parse(final CallContext context, final ParamConverter converter, final Object message) {
      // TODO Auto-generated method stub
      return null;
    }
  }

  @Retention(RUNTIME)
  @Target(TYPE)
  @interface TestActionA {
    String name();
  }

  @Retention(RUNTIME)
  @Target({TYPE, METHOD})
  @interface TestActionB {
    String name();
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface TestParamA {
    String name();
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface TestParamB {
    String name();
  }

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @interface TestIntParam {
  }
}
