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

import java.util.Arrays;
import java.util.BitSet;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.LongValue;
import tech.dnaco.collections.arrays.ArraySortUtil;
import tech.dnaco.data.CborFormat;
import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.geo.LatLong;
import tech.dnaco.storage.format.FieldFormatReader;

public final class FieldFormatReaderV0 implements FieldFormatReader {
  private final BitSet nullBitmap;
  private final int[] fieldIndex;

  private ByteArraySlice block;
  private int fieldIndexSize;

  public FieldFormatReaderV0(final int fieldsCount) {
    this.nullBitmap = new BitSet(fieldsCount);
    this.fieldIndex = new int[fieldsCount * 2];
    this.fieldIndexSize = 0;
  }

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public void load(final ByteArraySlice blockSlice) {
    this.block = blockSlice; // TODO: avoid copy
    this.nullBitmap.set(0, nullBitmap.size());
    Arrays.fill(fieldIndex, -1);
    this.fieldIndexSize = 0;

    final LongValue result = new LongValue();
    final int blockJumper = block.length() - 3;
    final int indexLength = Math.toIntExact(IntDecoder.LITTLE_ENDIAN.readFixed(block, blockJumper, 3));
    int indexOffset = blockJumper - indexLength;
    //System.out.println("INDEX OFFSET: " + indexOffset + " LENGTH " + indexLength + " block " + blockSlice.length());
    while (indexOffset < blockJumper) {
      indexOffset += VarInt.read(block, indexOffset, blockJumper - indexOffset, result);
      final int fieldValue = result.intValue();
      fieldIndex[fieldIndexSize++] = fieldValue >>> 1;
      nullBitmap.set(fieldValue >>> 1, (fieldValue & 1) != 0);

      indexOffset += VarInt.read(block, indexOffset, blockJumper - indexOffset, result);
      fieldIndex[fieldIndexSize++] = result.intValue();
    }
    //System.out.println(" -> " + Arrays.toString(fieldIndex));
  }

  // ================================================================================
  //  Primitive Types
  // ================================================================================
  @Override
  public boolean isNull(final int fieldId) {
    return fieldId >= nullBitmap.size() || nullBitmap.get(fieldId);
  }

  @Override
  public boolean getBool(final int fieldId) {
    final int offset = offsetByFieldId(fieldId);
    return DataFormatV0.getBool(block, offset);
  }

  @Override
  public long getInt(final int fieldId) {
    final int offset = offsetByFieldId(fieldId);
    return DataFormatV0.getInt(block, offset);
  }

  @Override
  public double getFloat(final int fieldId) {
    final int offset = offsetByFieldId(fieldId);
    return DataFormatV0.getFloat(block, offset);
  }

  @Override
  public ByteArraySlice getBytes(final int fieldId) {
    final int offset = offsetByFieldId(fieldId);
    return DataFormatV0.getBytes(block, offset);
  }

  @Override
  public JsonArray getArray(final int fieldId) {
    final ByteArraySlice value = getBytes(fieldId);
    return CborFormat.INSTANCE.fromBytes(value, JsonArray.class);
  }

  @Override
  public JsonObject getObject(final int fieldId) {
    final ByteArraySlice value = getBytes(fieldId);
    return CborFormat.INSTANCE.fromBytes(value, JsonObject.class);
  }

  // ================================================================================
  //  Geo Types
  // ================================================================================
  @Override
  public LatLong getGeoLocation(final int fieldId) {
    final int offset = offsetByFieldId(fieldId);
    return DataFormatV0.getGeoLocation(block, offset);
  }

  // ================================================================================
  //  PRIVATE helpers
  // ================================================================================
  private int indexByFieldId(final int fieldId) {
    return ArraySortUtil.binarySearch(fieldIndex, 0, fieldIndexSize >>> 1, 2, fieldId) * 2;
  }

  private int offsetByFieldId(final int fieldId) {
    return fieldIndex[indexByFieldId(fieldId) + 1];
  }
}
