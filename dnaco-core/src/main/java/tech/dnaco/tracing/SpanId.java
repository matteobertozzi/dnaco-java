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

import java.util.Objects;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.strings.BaseN;
import tech.dnaco.util.RandData;

public final class SpanId implements Comparable<SpanId> {
  private final long spanId;
  private String encoded;

  public SpanId(final long spanId) {
    this.spanId = spanId;
    this.encoded = null;
  }

  public long getSpanId() {
    return spanId;
  }

  public static SpanId newRandomId() {
    final byte[] randomBytes = new byte[8];
    do {
      RandData.generateBytes(randomBytes);
    } while (BytesUtil.isFilledWithZeros(randomBytes));
    return fromBytes(randomBytes);
  }

  public static SpanId fromBytes(final byte[] spanId) {
    if (spanId == null || spanId.length != 8) {
      throw new IllegalArgumentException("expected a 64bit SpanId");
    }

    long v = 0;
    for (int i = 0; i < 8; ++i) {
      v = (v << 8) | (spanId[i] & 0xff);
    }
    return new SpanId(v);
  }

  public static SpanId fromString(final String spanId) {
    return new SpanId(BaseN.decodeBase58(spanId));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(spanId);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof SpanId)) return false;

    final SpanId other = (SpanId) obj;
    return spanId == other.spanId;
  }

  @Override
  public int compareTo(final SpanId other) {
    return Long.compareUnsigned(spanId, other.spanId);
  }

  @Override
  public String toString() {
    if (encoded != null) return encoded;

    encoded = BaseN.encodeBase58(spanId);
    return encoded;
  }
}
