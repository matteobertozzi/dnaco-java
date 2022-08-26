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

package tech.dnaco.dispatcher;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

public interface Actions {
  // ================================================================================
  // Method Annotations
  // ================================================================================
  /**
   * Use this when the method execution takes more than 5sec (2sec is already super slow for a server).
   * The execution will be placed inside the slow queue.
   * In this way other requests will not be stuck below this execution.
   */
  @Retention(RUNTIME)
  @Target(METHOD)
  @interface Slow {
  }

  /**
   * Use this when the method execution may be delayed a bit,
   * so other calls can just in front when queued.
   */
  @Retention(RUNTIME)
  @Target(METHOD)
  @interface LowPriority {
  }

  /**
   * Use this ONLY if the method should be executed before the others in the queue.
   * there should be only a few (2/3) methods like marked as @HighPriority otherwise
   * you are not getting any benefit since every @HighPriority method will be enqueued.
   */
  @Retention(RUNTIME)
  @Target(METHOD)
  @interface HighPriority {
  }

  /**
   * Use this ONLY if the method takes less than 1ms.
   * If the method takes longer YOU are blocking the server.
   */
  @Retention(RUNTIME)
  @Target(METHOD)
  @interface InlineFast {
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  @interface AsyncResult {
  }
}
