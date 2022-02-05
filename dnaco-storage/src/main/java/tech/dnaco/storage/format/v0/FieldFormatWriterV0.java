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

package tech.dnaco.storage.format.v0;

import com.fasterxml.jackson.core.JsonProcessingException;

import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.arrays.ArraySortUtil;
import tech.dnaco.collections.arrays.IntArray;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.data.CborFormat;
import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.storage.format.FieldFormatWriter;

public final class FieldFormatWriterV0 implements FieldFormatWriter {
  private final PagedByteArray buffer;
  private final IntArray fieldIndex;
  private int blockOffset;

  public FieldFormatWriterV0(final PagedByteArray buffer, final int fieldsCount) {
    this.buffer = buffer;
    this.fieldIndex = new IntArray(fieldsCount * 2);
    this.blockOffset = buffer.size();
  }

  @Override
  public int getVersion() {
    return 0;
  }

  public void reset() {
    this.fieldIndex.reset();
    this.blockOffset = buffer.size();
  }

  @Override
  public void close() {
    // sort fieldIds & offsets
    ArraySortUtil.sort(0, fieldIndex.size() / 2, (a, b) -> Long.compare(fieldIndex.get(a * 2) >>> 1, fieldIndex.get(b * 2) >>> 1), (a, b) -> {
      final int aIndex = a * 2;
      final int bIndex = b * 2;
      fieldIndex.swap(aIndex, bIndex);
      fieldIndex.swap(aIndex + 1, bIndex + 1);
    });

    // write [fieldId:offset]
    final int offset = buffer.size();
    for (int i = 0, n = fieldIndex.size(); i < n; i += 2) {
      VarInt.write(buffer, fieldIndex.get(i));
      VarInt.write(buffer, fieldIndex.get(i + 1) - blockOffset);
    }
    //System.out.println("W:INDEX OFFSET: " + offset + " -> " + (buffer.size() - offset) + " -> " + fieldIndex);
    buffer.addFixed(3, buffer.size() - offset);
  }

  // ================================================================================
  //  Primitive Types
  // ================================================================================
  @Override
  public void writeNull(final int fieldId) {
    fieldIndex.add(fieldId << 1 | 1);
    fieldIndex.add(buffer.size());
  }

  @Override
  public void writeBool(final int fieldId, final boolean value) {
    writeField(fieldId);
    DataFormatV0.writeBool(buffer, value);
  }

  @Override
  public void writeInt(final int fieldId, final long value) {
    writeField(fieldId);
    DataFormatV0.writeInt(buffer, value);
  }

  @Override
  public void writeFloat(final int fieldId, final double value) {
    writeField(fieldId);
    DataFormatV0.writeFloat(buffer, value);
  }

  @Override
  public void writeBytes(final int fieldId, final byte[] value, final int off, final int len) {
    writeField(fieldId);
    DataFormatV0.writeBytes(buffer, value, off, len);
  }

  @Override
  public void writeArray(final int fieldId, final JsonArray value) {
    try {
      writeBytes(fieldId, CborFormat.INSTANCE.asBytes(value));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeObject(final int fieldId, final JsonObject value) {
    try {
      writeBytes(fieldId, CborFormat.INSTANCE.asBytes(value));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  // ================================================================================
  //  Geo Types
  // ================================================================================
  @Override
  public void writeGeoLocation(final int fieldId, final double latitude, final double longitude) {
    writeField(fieldId);
    DataFormatV0.writeGeoLocation(buffer, latitude, longitude);
  }

  // ================================================================================
  //  PRIVATE helpers
  // ================================================================================
  private void writeField(final int fieldId) {
    fieldIndex.add(fieldId << 1);
    fieldIndex.add(buffer.size());
  }
}
