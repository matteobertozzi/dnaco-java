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
import java.util.EnumSet;

import tech.dnaco.storage.entity.StorageEntity;
import tech.dnaco.storage.entity.StorageEntityKeyValueCoder.StorageEntityKeyValueDecoder;
import tech.dnaco.storage.entity.StorageEntityKeyValueCoder.StorageEntityKeyValueEncoder;
import tech.dnaco.storage.entity.StorageEntityType;
import tech.dnaco.storage.entity.StorageKeyValue;

public class TestEntity implements StorageEntity {
  public static void main(String[] args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    TestModelA model = new TestModelA();
    model.setKayA("aaa-key");
    model.setKayB(10);
    model.setFieldString("fs");
    model.setFieldInt(123);
    model.setFieldLong(456);
    model.setFieldBytes("hello".getBytes());
    model.setFieldStringArray(new String[] { "zzz", "www", "kkk" });
    model.setFieldEnum(EnumSet.of(TestModelA.TestEnum.BBB, TestModelA.TestEnum.CCC));

    StorageEntityType entityType = StorageEntityType.getStorageEntityType(TestModelA.class);
    StorageEntityKeyValueEncoder encoder = new StorageEntityKeyValueEncoder();
    entityType.encode(model, encoder);
    for (StorageKeyValue kv: encoder.getKeyValues()) {
      System.out.println(" -> " + kv);
    }

    TestModelA entityNew = new TestModelA();
    StorageEntityKeyValueDecoder decoder = new StorageEntityKeyValueDecoder(encoder.getKeyValues());
    entityType.decode(entityNew, decoder);
    System.out.println(entityNew);
  }
}