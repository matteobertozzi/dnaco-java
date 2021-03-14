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

package tech.dnaco.net.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import tech.dnaco.data.DataFormat;

public final class ByteBufDataFormatUtil {
  private ByteBufDataFormatUtil() {
    // no-op
  }

  public static <T> T fromBytes(final DataFormat format, final ByteBuf data, final Class<T> valueType) throws IOException {
    final InputStream stream = new ByteBufInputStream(data);
    try {
      return format.fromStream(stream, valueType);
    } finally {
      stream.reset();
    }
  }

  public static void addToBytes(final DataFormat format, final ByteBuf buffer, final Object obj) throws IOException {
    final OutputStream stream = new ByteBufOutputStream(buffer);
    format.addToStream(stream, obj);
    stream.flush();
  }
}
