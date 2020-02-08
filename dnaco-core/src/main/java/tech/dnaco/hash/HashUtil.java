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

package tech.dnaco.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import tech.dnaco.bytes.BytesUtil;

public final class HashUtil {
  private HashUtil() {
    // no-op
  }

  public static byte[] hash(final String algo, final byte[] buf) {
    return hash(algo, buf, 0, BytesUtil.length(buf));
  }

  public static byte[] hash(final String algo, final byte[] buf, final int off, final int len) {
    try {
      final MessageDigest digest = MessageDigest.getInstance(algo);
      digest.update(buf, off, len);
      return digest.digest();
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
