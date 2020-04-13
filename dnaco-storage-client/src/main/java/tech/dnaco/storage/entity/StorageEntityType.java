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

package tech.dnaco.storage.entity;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.JsonObject;

import tech.dnaco.storage.entity.StorageEntity.StorageKeyField;
import tech.dnaco.strings.StringConverter;

public class StorageEntityType {
  private final Class<? extends StorageEntity> entityClass;
  private final String[] keyNames;
  private final Method[] keySetMethods;
  private final Method[] keyGetMethods;
  private final String[] fieldNames;
  private final Method[] fieldSetMethods;
  private final Method[] fieldGetMethods;
  private final Method[] fieldChangedMethods;

  private static <T extends StorageEntity> StorageEntityType build(final Class<T> entity) {
    // extract fields
    final Field[] fields = entity.getDeclaredFields();
    final ArrayList<KeyInfo> keyNames = new ArrayList<>();
    final ArrayList<String> dataFields = new ArrayList<>(fields.length);
    for (int i = 0; i < fields.length; ++i) {
      final Field field = fields[i];
      if (field.isAnnotationPresent(StorageKeyField.class)) {
        final StorageKeyField key = field.getAnnotation(StorageKeyField.class);
        keyNames.add(new KeyInfo(field.getName(), key.index()));
      } else {
        dataFields.add(field.getName());
      }
    }

    // sort fields
    Collections.sort(keyNames);
    Collections.sort(dataFields);

    // map methods
    final Method[] methods = entity.getDeclaredMethods();
    final HashMap<String, Method> methodMap = new HashMap<>(methods.length, 1.0f);
    for (int i = 0; i < methods.length; ++i) {
      final Method m = methods[i];
      methodMap.put(m.getName(), m);
    }

    return new StorageEntityType(entity, methodMap, keyNames, dataFields);
  }

  private StorageEntityType(final Class<? extends StorageEntity> entityClass, final HashMap<String, Method> methodMap,
      final ArrayList<KeyInfo> keyFields, final ArrayList<String> dataFields) {
    this.entityClass = entityClass;
    this.keyNames = new String[keyFields.size()];
    this.keySetMethods = new Method[keyFields.size()];
    this.keyGetMethods = new Method[keyFields.size()];
    this.fieldNames = new String[dataFields.size()];
    this.fieldSetMethods = new Method[dataFields.size()];
    this.fieldGetMethods = new Method[dataFields.size()];
    this.fieldChangedMethods = new Method[dataFields.size()];

    for (int i = 0, n = keyFields.size(); i < n; ++i) {
      final KeyInfo keyInfo = keyFields.get(i);
      final String name = StringConverter.snakeToCamelCase(keyInfo.name);
      keyNames[i] = name;
      keyGetMethods[i] = methodMap.get("get" + name);
      keySetMethods[i] = methodMap.get("set" + name);
    }

    for (int i = 0, n = dataFields.size(); i < n; ++i) {
      final String name = StringConverter.snakeToCamelCase(dataFields.get(i));
      fieldNames[i] = name;
      fieldGetMethods[i] = methodMap.get("get" + name);
      fieldSetMethods[i] = methodMap.get("set" + name);
      fieldChangedMethods[i] = methodMap.get("has" + name + "Changes");
    }
  }

  public <T extends StorageEntity> void encode(final T entity, final StorageEntityEncoder encoder)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    encodeKey(entity, encoder);
    encodeFields(entity, encoder);
  }

  public <T extends StorageEntity> void encodeKey(final T entity, final StorageEntityEncoder encoder)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    for (int i = 0; i < keyNames.length; ++i) {
      final Method getter = keyGetMethods[i];
      encoder.addKeyField(keyNames[i], i, getter.getReturnType(), getter.invoke(entity));
    }
  }

  public <T extends StorageEntity> void encodeFields(final T entity, final StorageEntityEncoder encoder)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    for (int i = 0; i < fieldNames.length; ++i) {
      final Method getter = fieldGetMethods[i];
      encoder.addValueField(fieldNames[i], getter.getReturnType(), getter.invoke(entity));
    }
  }

  public <T extends StorageEntity> void decode(final T entity, final StorageEntityDecoder decoder)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    decodeKey(entity, decoder);
    decodeFields(entity, decoder);
  }

  public <T extends StorageEntity> void decodeKey(final T entity, final StorageEntityDecoder decoder)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    for (int i = 0; i < keyNames.length; ++i) {
      final Object v = decoder.getKeyField(keyNames[i], i, keyGetMethods[i].getReturnType());
      keySetMethods[i].invoke(entity, v);
    }
  }

  public <T extends StorageEntity> void decodeFields(final T entity, final StorageEntityDecoder decoder)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    for (int i = 0; i < fieldNames.length; ++i) {
      final Object v = decoder.getValueField(fieldNames[i], fieldGetMethods[i].getReturnType());
      fieldSetMethods[i].invoke(entity, v);
    }
  }

  private static final class KeyInfo implements Comparable<KeyInfo> {
    private final String name;
    private final int index;

    private KeyInfo(final String name, final int index) {
      this.name = name;
      this.index = index;
    }

    @Override
    public int compareTo(final KeyInfo other) {
      final int cmp = Integer.compare(index, other.index);
      return cmp != 0 ? cmp : name.compareTo(other.name);
    }
  }

  // ================================================================================
  private static final ConcurrentHashMap<Class<? extends StorageEntity>, StorageEntityType> entityTypeCache = new ConcurrentHashMap<>();

  public static <T extends StorageEntity> StorageEntityType getStorageEntityType(final Class<T> entityClass) {
    return entityTypeCache.computeIfAbsent(entityClass, (clz) -> StorageEntityType.build(clz));
  }

  public static <T extends StorageEntity> JsonObject encodeToJsonObject(final T entity)
      throws StorageEntityException {
    try {
      final StorageEntityType entityType = getStorageEntityType(entity.getClass());

      final StorageEntityJsonCoder encoder = new StorageEntityJsonCoder();
      entityType.encode(entity, encoder);
      return encoder.toJsonObject();
    } catch (Exception e) {
      throw new StorageEntityException(e);
    }
  }

  public static <T extends StorageEntity> T decodeFromJson(final JsonObject data, final Class<T> entityClass)
      throws StorageEntityException {
    try {
      final StorageEntityType entityType = getStorageEntityType(entityClass);

      final StorageEntityJsonCoder decoder = new StorageEntityJsonCoder(data);
      final T entity = entityClass.getDeclaredConstructor().newInstance();
      entityType.decode(entity, decoder);
      return entity;
    } catch (Exception e) {
      throw new StorageEntityException(e);
    }
  }
}