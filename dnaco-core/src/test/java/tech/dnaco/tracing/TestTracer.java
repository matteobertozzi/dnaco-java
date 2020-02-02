/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.tracing;

import org.junit.jupiter.api.Test;

public class TestTracer {
  @Test
  public void test() throws InterruptedException {
    testTraceSession("FOO");
    testTraceSession("BAR");

    try (final Tracer tracer = Tracer.newSessionTracer("livvo", "test func")) {
      System.out.println(Traces.humanReport());
      System.out.println(Traces.toJson());
    }
  }

  private void testTraceSession(final String sessionId) throws InterruptedException {
    try (final Tracer tracer = Tracer.newSessionTracer(sessionId, "test func")) {
      final Thread t = new Thread(() -> callB(sessionId));
      t.start();
      final long x = System.currentTimeMillis();
      while ((System.currentTimeMillis() - x) < 1000);
      callA();
      while ((System.currentTimeMillis() - x) < 2000);
      t.join();
    }
  }

  private void callA() {
    try (Tracer tracer = Tracer.newLocalTracer("a func")) {
      callA1();
    }
  }

  private void callA1() {
    try (Tracer tracer = Tracer.newLocalTracer("a1 func")) {
      final long x = System.currentTimeMillis();
      while ((System.currentTimeMillis() - x) < 500);
    }
  }

  private void callB(final String sessionId) {
    try (Tracer tracer = Tracer.newTracer(Traces.getSession(sessionId), "b-func")) {
      // call3
      final long x = System.currentTimeMillis();
      while ((System.currentTimeMillis() - x) < 1500);
    }
  }
}
