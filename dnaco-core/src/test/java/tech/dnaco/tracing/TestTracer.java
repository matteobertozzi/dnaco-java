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

package tech.dnaco.tracing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestTracer {
  @BeforeAll
  public static void beforeAll() {
    Tracer.setProvider(new NoOpTracingProvider());
  }

  @Test
  public void testTask() {
    final Span spanA = Tracer.newTask();
    Assertions.assertEquals(spanA, Tracer.getCurrentTask());

    final Span spanB = Tracer.newTask();
    Assertions.assertEquals(spanB, Tracer.getCurrentTask());
  }

  @Test
  public void testSpan() {
    final Span rootSpan = Tracer.newTask();
    final Span spanA = rootSpan.startSpan();
    Assertions.assertEquals(rootSpan, Tracer.getCurrentTask());
    Assertions.assertEquals(spanA, Tracer.getCurrentSpan());

    final Span spanB = rootSpan.startSpan();
    Assertions.assertEquals(rootSpan, Tracer.getCurrentTask());
    Assertions.assertEquals(spanB, Tracer.getCurrentSpan());
  }

  @Test
  public void testInner() {
    try (final Span span = Tracer.newTask()) {
      System.out.println("---> 1");
      try (final Span span2 = Tracer.newTask()) {
        System.out.println("---> 2");
      }
      System.out.println("---> 3");
    }
  }
}
