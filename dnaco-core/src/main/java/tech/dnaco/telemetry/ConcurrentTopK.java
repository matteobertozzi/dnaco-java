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

import tech.dnaco.tracing.TraceId;

public class ConcurrentTopK extends TopK {
	public ConcurrentTopK(final TopType type, final int k) {
		super(type, k);
  }

  @Override
  public void add(final String key, final long value, final TraceId traceId) {
    synchronized (this) {
      super.add(key, value, traceId);
    }
  }

  @Override
  public void clear() {
    synchronized (this) {
      super.clear();
    }
  }

  @Override
  public TopKData getSnapshot() {
    synchronized (this) {
      return super.getSnapshot();
    }
  }
}
