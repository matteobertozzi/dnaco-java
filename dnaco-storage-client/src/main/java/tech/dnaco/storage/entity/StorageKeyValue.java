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

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public class StorageKeyValue extends AbstractReferenceCounted {
  private ByteBuf key;
  private ByteBuf value;

  public StorageKeyValue(final ByteBuf key, final ByteBuf value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public ReferenceCounted touch(final Object hint) {
    return this;
  }

  @Override
  protected void deallocate() {
    key.release();
    value.release();
  }

  public ByteBuf getKey() {
    return key;
  }

  public void setKey(final ByteBuf key) {
    this.key = key;
  }

  public ByteBuf getValue() {
    return value;
  }

  public void setValue(final ByteBuf value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "StorageKeyValue [key=" + key.toString(StandardCharsets.UTF_8) + ", value=" + value.toString(StandardCharsets.UTF_8) + "]";
  }
}