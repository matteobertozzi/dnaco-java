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

package tech.dnaco.telemetry;

import java.util.concurrent.TimeUnit;

public class ConcurrentTimeRangeGauge extends ConcurrentTimeRangeCounter {
  public ConcurrentTimeRangeGauge(final long maxInterval, final long window, final TimeUnit unit) {
    super(maxInterval, window, unit);
  }

  @Override
  protected long computeNewValue(final long newValue) {
    return getPrevValue() + newValue;
  }

  @Override
  protected void injectZeros(final long now) {
    injectZeros(now, true);
  }
}
