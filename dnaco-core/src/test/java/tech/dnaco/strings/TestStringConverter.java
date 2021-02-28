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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestStringConverter {
  @Test
  public void testString() {
    assertEquals("aaa_bbb_ccc", StringConverter.camelToSnakeLowerCase("AaaBbbCcc"));
    assertEquals("AAA_BBB_CCC", StringConverter.camelToSnakeUpperCase("AaaBbbCcc"));
    assertEquals("aaa_bbb_ccc_d", StringConverter.camelToSnakeLowerCase("AaaBbbCccD"));
    assertEquals("AAA_BBB_CCC_D", StringConverter.camelToSnakeUpperCase("AaaBbbCccD"));
    assertEquals("AaaBbbCcc", StringConverter.snakeToCamelCase("aaa_bbb_ccc"));
    assertEquals("AaaBbbCcc", StringConverter.snakeToCamelCase("aaa_bbb_ccc_"));
  }

  @Test
  public void testInt() {
    assertEquals(10, StringConverter.toInt("10", 0));
    assertEquals(20, StringConverter.toInt(" 20", 0));
    assertEquals(30, StringConverter.toInt(" 30 ", 0));
    assertEquals(40, StringConverter.toInt("40  ", 0));

    assertEquals(1, StringConverter.toInt(null, 1));
    assertEquals(2, StringConverter.toInt("", 2));
    assertEquals(3, StringConverter.toInt(" ", 3));
    assertEquals(4, StringConverter.toInt("foo ", 4));
  }

  @Test
  public void testLong() {
    assertEquals(100000000000L, StringConverter.toLong("100000000000", 0));
    assertEquals(200000000000L, StringConverter.toLong(" 200000000000", 0));
    assertEquals(300000000000L, StringConverter.toLong(" 300000000000 ", 0));
    assertEquals(400000000000L, StringConverter.toLong("400000000000  ", 0));

    assertEquals(100000000000L, StringConverter.toLong(null, 100000000000L));
    assertEquals(200000000000L, StringConverter.toLong("", 200000000000L));
    assertEquals(300000000000L, StringConverter.toLong(" ", 300000000000L));
    assertEquals(400000000000L, StringConverter.toLong("foo ", 400000000000L));
  }

  @Test
  public void testFloat() {
    assertEquals(1.5f, StringConverter.toFloat("1,5", 0), 0.1f);
    assertEquals(2.5f, StringConverter.toFloat("2.5", 0), 0.1f);

    assertEquals(3.7f, StringConverter.toFloat("", 3.7f), 0.1f);
    assertEquals(4.9f, StringConverter.toFloat(" ", 4.9f), 0.1f);
    assertEquals(5.5f, StringConverter.toFloat(null, 5.5f), 0.1f);
    assertEquals(6.2f, StringConverter.toFloat("foo", 6.2f), 0.1f);
  }

  @Test
  public void testDouble() {
    assertEquals(1.5, StringConverter.toDouble("1,5", 0), 0.1f);
    assertEquals(2.5, StringConverter.toDouble("2.5", 0), 0.1f);

    assertEquals(3.7, StringConverter.toDouble("", 3.7), 0.1f);
    assertEquals(4.9, StringConverter.toDouble(" ", 4.9), 0.1f);
    assertEquals(5.5, StringConverter.toDouble(null, 5.5), 0.1f);
    assertEquals(6.2, StringConverter.toDouble("foo", 6.2), 0.1f);
  }

  @Test
  public void testBoolean() {
    assertFalse(StringConverter.toBoolean("false", true));
    assertFalse(StringConverter.toBoolean("False", true));
    assertFalse(StringConverter.toBoolean("  False", true));
    assertTrue(StringConverter.toBoolean("true", false));
    assertTrue(StringConverter.toBoolean("True", false));
    assertTrue(StringConverter.toBoolean("  True", false));
    assertFalse(StringConverter.toBoolean("0", true));
    assertTrue(StringConverter.toBoolean("1", false));

    assertFalse(StringConverter.toBoolean("nau", false));
    assertTrue(StringConverter.toBoolean("bau", true));
    assertFalse(StringConverter.toBoolean("", false));
    assertTrue(StringConverter.toBoolean("", true));
    assertFalse(StringConverter.toBoolean(null, false));
    assertTrue(StringConverter.toBoolean(null, true));
  }
}
