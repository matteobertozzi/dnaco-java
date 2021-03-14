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

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.strings.BaseN;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.RandData;

public class TraceId implements Comparable<TraceId> {
  public static final TraceId NULL_TRACE_ID = new TraceId(0, 0);

  private static final char TRACE_ID_SEPARATOR = '-';

  private final long hi;
  private final long lo;
  private String encoded;

  public TraceId(final long hi, final long lo) {
    this.hi = hi;
    this.lo = lo;
    this.encoded = null;
  }

  public long getHi() {
    return hi;
  }

  public long getLo() {
    return lo;
  }

  public static TraceId newRandomId() {
    final byte[] randomBytes = new byte[16];
    do {
      RandData.generateBytes(randomBytes);
    } while (BytesUtil.isFilledWithZeros(randomBytes));
    return fromBytes(randomBytes);
  }

  public static TraceId fromBytes(final byte[] traceId) {
    if (traceId == null || traceId.length != 16) {
      throw new IllegalArgumentException("expected a 128bit TraceId");
    }

    long hi = 0;
    long lo = 0;
    for (int i = 0; i < 8; ++i) hi = (hi << 8) | (traceId[i] & 0xff);
    for (int i = 8; i < 16; ++i) lo = (lo << 8) | (traceId[i] & 0xff);
    return new TraceId(hi, lo);
  }

  public static TraceId fromString(final String traceId) {
    if (StringUtil.isEmpty(traceId)) return null;

    if (traceId.length() == 32) {
      return fromBytes(BytesUtil.fromHexString(traceId));
    }

    final int separatorIndex = traceId.indexOf(TRACE_ID_SEPARATOR);
    final long hi = BaseN.decodeBase58(traceId, 0, separatorIndex);
    final long lo = BaseN.decodeBase58(traceId, separatorIndex + 1, traceId.length() - 1 - separatorIndex);
    return new TraceId(hi, lo);
  }

  @Override
  public int hashCode() {
    final long hilo = hi ^ lo;
    return ((int)(hilo >> 32)) ^ (int) hilo;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof TraceId)) return false;

    final TraceId other = (TraceId)obj;
    return hi == other.hi && lo == other.lo;
  }

  @Override
  public int compareTo(final TraceId other) {
    final int cmp = Long.compareUnsigned(this.hi, other.hi);
    return cmp != 0 ? cmp : Long.compareUnsigned(this.lo, other.lo);
  }

  @Override
  public String toString() {
    if (encoded != null) return encoded;

    final StringBuilder builder = new StringBuilder(23);
    BaseN.encodeBase58(builder, lo);
    builder.append(TRACE_ID_SEPARATOR);
    BaseN.encodeBase58(builder, hi);
    encoded = builder.reverse().toString();
    return encoded;
  }
}