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

package tech.dnaco.threading;

import java.util.concurrent.atomic.AtomicBoolean;

public interface RunningState {
  boolean isRunning();

  class ServiceRunningState implements RunningState {
    private final AtomicBoolean running;

    public ServiceRunningState() {
      this(true);
    }

    public ServiceRunningState(final boolean state) {
      this(new AtomicBoolean(state));
    }

    public ServiceRunningState(final AtomicBoolean running) {
      this.running = running;
    }

    public void start() {
      running.set(true);
    }

    public void stop() {
      running.set(false);
    }

    @Override
    public boolean isRunning() {
      return running.get();
    }
  }
}
