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

package tech.dnaco.data;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.data.json.JsonElement;
import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.util.Serialization.SerializeWithSnakeCase;

public class TestJsonUtil {
  @Test
  public void testJsonElement() {
    final JsonElement element = JsonUtil.fromJson("[1, 2, 3]", JsonElement.class);
    System.out.println(element);
    System.out.println(element.toTreeNode());
  }

  @Test
  public void testJsonTree() {
    final TestData refData = new TestData();
    refData.longNotSerialized = System.currentTimeMillis();
    refData.boolValue = true;
    refData.bytesValue = new byte[] { 1, 2, 3, 4 };
    refData.enumValue = TestEnum.EA;
    refData.subData = new TestSubData(10, "subx");
    refData.intList = List.of(1, 2, 3, 4);
    refData.setLong = Set.of(0xffffffffffffL, 0xfffffffffffffL, 0xffffffffffffffL);
    refData.strSet = Set.of("aaa", "bbb", "ccc");
    refData.enumSetValue = EnumSet.of(TestEnum.EB, TestEnum.EC);
    refData.mapStrLong = Map.of("Aa", 10L, "Bb", 20L);
    refData.mapIntSubData = Map.of(1, new TestSubData(1, "un"), 2, new TestSubData(2, "du"));

    Assertions.assertEquals(refData, JsonUtil.fromJson(JsonUtil.toJson(refData), TestData.class));
    Assertions.assertEquals(refData, JsonUtil.fromJson(JsonUtil.toJson(JsonUtil.toJsonTree(refData)), TestData.class));
  }

  @Test
  public void testFoo() {

    final TestData refData = new TestData();
    refData.longNotSerialized = System.currentTimeMillis();
    refData.boolValue = true;
    refData.bytesValue = new byte[] { 1, 2, 3, 4 };
    refData.enumValue = TestEnum.EA;
    refData.subData = new TestSubData(10, "subx");
    refData.intList = List.of(1, 2, 3, 4);
    refData.setLong = Set.of(0xffffffffffffL, 0xfffffffffffffL, 0xffffffffffffffL);
    refData.strSet = Set.of("aaa", "bbb", "ccc");
    refData.enumSetValue = EnumSet.of(TestEnum.EB, TestEnum.EC);
    refData.mapStrLong = Map.of("Aa", 10L, "Bb", 20L);
    refData.mapIntSubData = Map.of(1, new TestSubData(1, "un"), 2, new TestSubData(2, "du"));

    System.out.println(JsonUtil.toJson(new TestData()));
  }


  public enum TestEnum { EA, EB, EC }

  public static final class TestSubData {
    private transient String strNotSerialized;

    private int a;
    private String b;

    public TestSubData() {
      // no-op
    }

    public TestSubData(final int a, final String b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public int hashCode() {
      return Objects.hash(a, b, strNotSerialized);
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj)
        return true;
      if (!(obj instanceof TestSubData))
        return false;
      final TestSubData other = (TestSubData) obj;
      return a == other.a && Objects.equals(b, other.b) && Objects.equals(strNotSerialized, other.strNotSerialized);
    }

    @Override
    public String toString() {
      return "TestSubData [a=" + a + ", b=" + b + ", strNotSerialized=" + strNotSerialized + "]";
    }
  }

  @SerializeWithSnakeCase
  public static final class TestData {
    private transient long longNotSerialized;

    private boolean boolValue;
    private Boolean boolNullValue;
    private int intValue;
    private Integer intNullValue;
    private long longValue;
    private Long longNullValue;
    private float floatValue;
    private Float floatNullValue;
    private String strValue;
    private byte[] bytesValue;
    private TestEnum enumValue;
    private TestSubData subData;
    private List<Integer> intList;
    private List<String> strList;
    private Set<String> strSet;
    private Set<Long> setLong;
    private EnumSet<TestEnum> enumSetValue;
    private Map<String, Long> mapStrLong;
    private Map<Integer, TestSubData> mapIntSubData;

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(bytesValue);
      result = prime * result + Objects.hash(boolNullValue, boolValue, enumSetValue, enumValue, floatNullValue,
          floatValue, intList, intNullValue, intValue, longNullValue, longValue, mapIntSubData,
          mapStrLong, setLong, strList, strSet, strValue, subData);
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) return true;

      if (!(obj instanceof TestData)) return false;
      final TestData other = (TestData) obj;
      return Objects.equals(boolNullValue, other.boolNullValue) && boolValue == other.boolValue
          && Arrays.equals(bytesValue, other.bytesValue) && Objects.equals(enumSetValue, other.enumSetValue)
          && enumValue == other.enumValue && Objects.equals(floatNullValue, other.floatNullValue)
          && Float.floatToIntBits(floatValue) == Float.floatToIntBits(other.floatValue)
          && Objects.equals(intList, other.intList) && Objects.equals(intNullValue, other.intNullValue)
          && intValue == other.intValue
          && Objects.equals(longNullValue, other.longNullValue) && longValue == other.longValue
          && Objects.equals(mapIntSubData, other.mapIntSubData) && Objects.equals(mapStrLong, other.mapStrLong)
          && Objects.equals(setLong, other.setLong) && Objects.equals(strList, other.strList)
          && Objects.equals(strSet, other.strSet) && Objects.equals(strValue, other.strValue)
          && Objects.equals(subData, other.subData);
    }

    @Override
    public String toString() {
      return "TestData [boolNullValue=" + boolNullValue + ", boolValue=" + boolValue + ", bytesValue="
          + Arrays.toString(bytesValue) + ", enumSetValue=" + enumSetValue + ", enumValue=" + enumValue
          + ", floatNullValue=" + floatNullValue + ", floatValue=" + floatValue + ", intList=" + intList
          + ", intNullValue=" + intNullValue + ", intValue=" + intValue + ", longNotSerialized="
          + longNotSerialized + ", longNullValue=" + longNullValue + ", longValue=" + longValue
          + ", mapIntSubData=" + mapIntSubData + ", mapStrLong=" + mapStrLong + ", setLong=" + setLong
          + ", strList=" + strList + ", strSet=" + strSet + ", strValue=" + strValue + ", subData=" + subData
          + "]";
    }
  }
}

