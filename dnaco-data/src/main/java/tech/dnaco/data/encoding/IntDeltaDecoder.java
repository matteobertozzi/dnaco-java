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

package tech.dnaco.data.encoding;

public class IntDeltaDecoder {
  private final IntReader intReader;

  private long lastValue;

  public IntDeltaDecoder(final IntReader intReader) {
    this.intReader = intReader;
  }

  public long readNext() {
    final long delta = intReader.readLong();
    final long value = lastValue + delta;

    lastValue = value;
    return value;
  }

  public long readFirst() {
    final long value = intReader.readLong();

    lastValue = value;
    return value;
  }
}
