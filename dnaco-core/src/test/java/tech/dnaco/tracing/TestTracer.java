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
  public void testTaskTracer() {
    // create a new task
    final TaskTracer taskTracer = Tracer.newTask();
    Assertions.assertEquals(taskTracer, Tracer.getCurrentTask());
    Assertions.assertEquals(taskTracer, Tracer.getTask(taskTracer.getTraceId()));
    taskTracer.close();

    // the task is closed, so no thread local
    Assertions.assertNull(Tracer.getCurrentTask());

    // the task is closed, so we will get a new instance
    final TaskTracer taskTracer2 = Tracer.getTask(taskTracer.getTraceId());
    Assertions.assertNotEquals(taskTracer, taskTracer2);
    Assertions.assertEquals(taskTracer2, Tracer.getCurrentTask());
    Assertions.assertEquals(taskTracer2, Tracer.getTask(taskTracer.getTraceId()));
  }
}
