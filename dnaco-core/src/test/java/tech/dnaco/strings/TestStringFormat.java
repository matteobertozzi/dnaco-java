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

package tech.dnaco.strings;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestStringFormat {
  @Test
  public void testBasic() {
    assertEquals("simple no format", StringFormat.format("simple no format"));
    assertEquals("foo 10 bar key:[20, 30] car v:30 zar", StringFormat.format("foo {} bar {key} car {v} zar", 10, new int[] { 20, 30 }, 30));
  }

  @Test
  public void testSupplier() {
    assertEquals("foo Lazy 1 bar key:10 car", StringFormat.format("foo {} bar {key} car", () -> "Lazy 1", () -> 10));
  }

  @Test
  public void testPositional() {
    assertEquals("simple no format", StringFormat.positionalFormat("simple no format"));
    assertEquals("foo XYZ bar 10 car XYZ zar", StringFormat.positionalFormat("foo {1} bar {0} car {1} zar", 10, "XYZ"));
  }

  @Test
  public void testFormat() {
    assertEquals("foo", applyFormat("foo"));
    assertEquals("10 foo", applyFormat("{} foo", 10));
    assertEquals("10 foo 20", applyFormat("{} foo {}", 10, 20));
    assertEquals("10 foo 20 bar 30", applyFormat("{} foo {} bar {}", 10, "20", 30));
  }

  @Test
  public void testPositionalFormat() {
    assertEquals("foo", StringFormat.positionalFormat("foo"));
    assertEquals("10 foo", StringFormat.positionalFormat("{0} foo", 10));
    assertEquals("10 foo 20", StringFormat.positionalFormat("{1} foo {0}", 20, 10));
    assertEquals("10 foo 20 bar 30", StringFormat.positionalFormat("{1} foo {0} bar {2}", "20", 10, 30));
  }

  private static String applyFormat(final String format, final Object... args) {
    final StringBuilder builder = new StringBuilder();
    StringFormat.applyFormat(builder, format, args);
    return builder.toString();
  }
}
