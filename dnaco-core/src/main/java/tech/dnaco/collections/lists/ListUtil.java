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

package tech.dnaco.collections.lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import tech.dnaco.collections.arrays.ArrayUtil;

public final class ListUtil {
  private ListUtil() {
    // no-op
  }

  public static <T> int size(final Collection<T> input) {
    return input != null ? input.size() : 0;
  }

  public static <T> boolean isEmpty(final Collection<T> input) {
    return input == null || input.isEmpty();
  }

  public static <T> boolean isNotEmpty(final Collection<T> input) {
    return input != null && !input.isEmpty();
  }

  public static <T> List<T> emptyIfNull(final List<T> input) {
    return input == null ? Collections.emptyList() : input;
  }

  public static <T> List<T> newArrayListIfNull(final List<T> input) {
    return input != null ? input : new ArrayList<>();
  }

  public static List<String> newArrayList(final List<String> input) {
    return input != null ? new ArrayList<>(input) : new ArrayList<>();
  }

  public static <T> T add(final Collection<T> list, final T value) {
    list.add(value);
    return value;
  }

  public static <T> void addAll(final Collection<T> list, final T[] values) {
    if (ArrayUtil.isEmpty(values)) return;

    for (int i = 0; i < values.length; ++i) {
      list.add(values[i]);
    }
  }

  public static <T> void addAll(final Collection<T> list, final Collection<T> values) {
    if (isNotEmpty(values)) {
      list.addAll(values);
    }
  }

  public static <T> List<T> subList(final List<T> records, final int offset, final int length) {
    if (isEmpty(records)) return records;
    if (offset > records.size()) return Collections.emptyList();

    final int endIndex = Math.min(records.size(), offset + length);
    return records.subList(offset, endIndex);
  }

  @SafeVarargs
  public static <T> List<T> concat(final List<T>... lists) {
    if(ArrayUtil.isEmpty(lists)) return Collections.emptyList();

    final ArrayList<T> ret = new ArrayList<>();

    for(int i = 0; i < lists.length; i ++) {
      ret.addAll(lists[i]);
    }
    return ret;
  }
}
