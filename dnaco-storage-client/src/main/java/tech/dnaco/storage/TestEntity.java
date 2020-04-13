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

package tech.dnaco.storage;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import tech.dnaco.storage.entity.StorageEntity;
import tech.dnaco.storage.entity.StorageEntityKeyValueCoder.StorageEntityKeyValueDecoder;
import tech.dnaco.storage.entity.StorageEntityKeyValueCoder.StorageEntityKeyValueEncoder;
import tech.dnaco.storage.entity.StorageEntityType;
import tech.dnaco.storage.entity.StorageKeyValue;


public class TestEntity implements StorageEntity {
  public enum TestEntityFields {
    BOOL_FIELD, INT_FIELD, LONG_FIELD, FLOAT_FIELD, DOUBLE_FIELD, STRING_FIELD,
    INT_ARRAY_FIELD, STIRNG_ARRAY_FIELD
  }

  @StorageKeyField(index = 0) private String key1;
  @StorageKeyField(index = 1) private int key2;

  private boolean bool_field;
  private int int_field;
  private long long_field;
  private float float_field;
  private double double_field;
  private String string_field;
  private byte[] byte_field;
  private int[] int_array_field;
  private String[] string_array_field;


  public boolean getBoolField() {
    return bool_field;
  }
  public void setBoolField(boolean boolField) {
    this.bool_field = boolField;
  }
  public int getIntField() {
    return int_field;
  }
  public void setIntField(int intField) {
    this.int_field = intField;
  }
  public long getLongField() {
    return long_field;
  }
  public void setLongField(long longField) {
    this.long_field = longField;
  }
  public float getFloatField() {
    return float_field;
  }
  public void setFloatField(float floatField) {
    this.float_field = floatField;
  }
  public double getDoubleField() {
    return double_field;
  }
  public void setDoubleField(double doubleField) {
    this.double_field = doubleField;
  }
  public String getStringField() {
    return string_field;
  }
  public void setStringField(String stringField) {
    this.string_field = stringField;
  }
  public byte[] getByteField() {
    return byte_field;
  }
  public void setByteField(byte[] byteField) {
    this.byte_field = byteField;
  }
  public int[] getIntArrayField() {
    return int_array_field;
  }
  public void setIntArrayField(int[] intArrayField) {
    this.int_array_field = intArrayField;
  }
  public String[] getStringArrayField() {
    return string_array_field;
  }
  public void setStringArrayField(String[] stringArrayField) {
    this.string_array_field = stringArrayField;
  }

  @Override
  public String toString() {
    return "TestEntity [bool_field=" + bool_field + ", byte_field=" + Arrays.toString(byte_field) + ", double_field="
        + double_field + ", float_field=" + float_field + ", int_array_field=" + Arrays.toString(int_array_field)
        + ", int_field=" + int_field + ", long_field=" + long_field + ", string_array_field="
        + Arrays.toString(string_array_field) + ", string_field=" + string_field + "]";
  }

  public String getKey1() {
    return key1;
  }
  public void setKey1(String key1) {
    this.key1 = key1;
  }
  public int getKey2() {
    return key2;
  }
  public void setKey2(int key2) {
    this.key2 = key2;
  }

  public static void main(String[] args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    TestEntity entity = new TestEntity();
    entity.setKey1("k1");
    entity.setKey2(10);
    entity.setBoolField(true);
    entity.setByteField("foo".getBytes());
    entity.setDoubleField(10.1);
    entity.setFloatField(11.2f);
    entity.setIntField(2);
    entity.setLongField(3);
    entity.setStringField("bar");

    StorageEntityType entityType = StorageEntityType.getStorageEntityType(TestEntity.class);
    StorageEntityKeyValueEncoder encoder = new StorageEntityKeyValueEncoder();
    entityType.encode(entity, encoder);
    for (StorageKeyValue kv: encoder.getKeyValues()) {
      System.out.println(" -> " + kv);
    }

    TestEntity entityNew = new TestEntity();
    StorageEntityKeyValueDecoder decoder = new StorageEntityKeyValueDecoder(encoder.getKeyValues());
    entityType.decode(entityNew, decoder);
    System.out.println(entityNew);
  }
}