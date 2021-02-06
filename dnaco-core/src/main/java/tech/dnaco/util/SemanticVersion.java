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

package tech.dnaco.util;

public final class SemanticVersion {
  private SemanticVersion() {
    // no-op
  }

  public static long compose(final int major, final int minor, final int patch) {
    if (patch < 0 || patch >= (1 << 23)) new IllegalArgumentException("patch must be 0-8388607: " + patch);
    if (minor < 0 || minor >= (1 << 20)) new IllegalArgumentException("patch must be 0-1048575: " + minor);
    if (major < 0 || major >= (1 << 20)) new IllegalArgumentException("major must be 0-1048575: " + major);
    return ((long)major << 43) | ((long)minor << 23) | patch;
  }

  public static int major(final long version) {
    return (int) ((version >> 43) & 0xfffff);
  }

  public static int minor(final long version) {
    return (int) ((version >> 23) & 0xfffff);
  }

  public static int patch(final long version) {
    return (int) (version & 0x7fffff);
  }

  public static String toString(final long version) {
    return major(version) + "." + minor(version) + "." + patch(version);
  }
}
