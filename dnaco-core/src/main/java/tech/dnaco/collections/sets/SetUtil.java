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

package tech.dnaco.collections.sets;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class SetUtil {
  private SetUtil() {
    // no-op
  }

  public static <T> int size(final Set<T> input) {
    return input != null ? input.size() : 0;
  }

  public static <T> boolean isEmpty(final Set<T> input) {
    return input == null || input.isEmpty();
  }

  public static <T> boolean isNotEmpty(final Set<T> input) {
    return input != null && !input.isEmpty();
  }

  public static <T> Set<T> emptyIfNull(final Set<T> input) {
    return input == null ? Collections.emptySet() : input;
  }


  public static <T> boolean contains(final Set<T> set, final T value) {
    return set != null && set.contains(value);
  }

  @SafeVarargs
  public static <T> boolean containsOneOf(final Set<T> set, final T... value) {
    if (isEmpty(set)) return false;

    for (int i = 0; i < value.length; ++i) {
      if (set.contains(value[i])) {
        return true;
      }
    }
    return false;
  }

  @SafeVarargs
  public static <T> Set<T> newHashSet(final T... items) {
    return new HashSet<>(List.of(items));
  }

  public static <T> void andAndEnsureLimit(final Set<T> set, final int limit, final T valueToAdd) {
    if (!set.add(valueToAdd)) return; // item was already in

    if (set.size() > limit) {
      set.remove(valueToAdd);
      throw new UnsupportedOperationException("unable to insert " + valueToAdd +
        ", the set has already reached the limit of " + limit);
    }
  }
}
