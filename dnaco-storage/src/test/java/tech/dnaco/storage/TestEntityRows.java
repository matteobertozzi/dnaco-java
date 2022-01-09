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

package tech.dnaco.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.storage.format.FieldFormatReader;
import tech.dnaco.storage.format.FieldFormatWriter;

public class TestEntityRows {
  @Test
  public void testWriteAndRead() {
    final int NROWS = 100_000;

    final Schema schema = new Schema();
    schema.addField("bool", DataType.BOOL);
    schema.addField("int", DataType.INT);
    schema.addField("float", DataType.FLOAT);

    // writing the data
    final EntityRowsWriter rows = new EntityRowsWriter(schema);
    for (int i = 1; i <= NROWS; ++i) {
      final FieldFormatWriter writer = rows.newRow();
      if (i % 3 == 0) {
        writer.writeNull(schema.fieldIdByName("bool"));
      } else {
        writer.writeBool(schema.fieldIdByName("bool"), i % 3 == 1);
      }

      if (i % 5 == 0) {
        writer.writeNull(schema.fieldIdByName("int"));
      } else {
        writer.writeInt(schema.fieldIdByName("int"), i * 100);
      }

      if (i % 7 == 0) {
        writer.writeNull(schema.fieldIdByName("float"));
      } else {
        writer.writeFloat(schema.fieldIdByName("float"), i * 100.32);
      }
      rows.closeRow();
    }
    rows.close();
    Assertions.assertEquals(2_457_607, rows.bufferSize());

    // Reading the data
    int count = 0;
    final EntityRowsReader rdRows = EntityRowsReader.newReader(new ByteArraySlice(rows.toByteArray()));
    while (rdRows.hasMore()) {
      final FieldFormatReader reader = rdRows.next();
      count++;

      if (count % 3 == 0) {
        Assertions.assertTrue(reader.isNull(schema.fieldIdByName("bool")));
      } else {
        Assertions.assertFalse(reader.isNull(schema.fieldIdByName("bool")));
        Assertions.assertEquals(count % 3 == 1, reader.getBool(schema.fieldIdByName("bool")));
      }

      if (count % 5 == 0) {
        Assertions.assertTrue(reader.isNull(schema.fieldIdByName("int")));
      } else {
        Assertions.assertFalse(reader.isNull(schema.fieldIdByName("int")));
        Assertions.assertEquals(count * 100L, reader.getInt(schema.fieldIdByName("int")));
      }

      if (count % 7 == 0) {
        Assertions.assertTrue(reader.isNull(schema.fieldIdByName("float")));
      } else {
        Assertions.assertFalse(reader.isNull(schema.fieldIdByName("float")));
        Assertions.assertEquals(count * 100.32, reader.getFloat(schema.fieldIdByName("float")));
      }
    }
    Assertions.assertEquals(NROWS, count);
  }

  @Test
  public void testCopy() {
    final Schema schemaA = new Schema();
    schemaA.addField("bool", DataType.BOOL);
    schemaA.addField("int", DataType.INT);
    schemaA.addField("float", DataType.FLOAT);

    final Schema schemaB = new Schema();
    schemaB.addField("a_1", DataType.STRING);
    schemaB.addField("float", DataType.FLOAT);
    schemaB.addField("b_2", DataType.STRING);
    schemaB.addField("bool", DataType.BOOL);
    schemaB.addField("c_3", DataType.STRING);
    schemaB.addField("int", DataType.INT);

    final int NROWS = 10;

    final EntityRowsWriter rowsA = new EntityRowsWriter(schemaA);
    for (int i = 1; i <= NROWS; ++i) {
      final FieldFormatWriter writer = rowsA.newRow();
      writer.writeInt(schemaA.fieldIdByName("int"), i * 100);
      writer.writeBool(schemaA.fieldIdByName("bool"), (i & 1) == 0);
      writer.writeFloat(schemaA.fieldIdByName("float"), i * 5.2);
      rowsA.closeRow();
    }
    rowsA.close();

    final byte[] rowsB = rewrite(schemaA, rowsA.toByteArray(), schemaB, NROWS);

    int count = 0;
    final EntityRowsReader rdRowsB = EntityRowsReader.newReader(new ByteArraySlice(rowsB));
    for (int i = 1; rdRowsB.hasMore(); ++i) {
      final FieldFormatReader reader = rdRowsB.next();
      Assertions.assertFalse(reader.isNull(schemaB.fieldIdByName("bool")));
      Assertions.assertEquals((i & 1) == 0, reader.getBool(schemaB.fieldIdByName("bool")));
      Assertions.assertFalse(reader.isNull(schemaB.fieldIdByName("int")));
      Assertions.assertEquals(i * 100, reader.getInt(schemaB.fieldIdByName("int")));
      Assertions.assertFalse(reader.isNull(schemaB.fieldIdByName("float")));
      Assertions.assertEquals(i * 5.2, reader.getFloat(schemaB.fieldIdByName("float")), 0.00001);
      count++;
    }
    Assertions.assertEquals(NROWS, count);
  }

  @Test
  public void testConvert() {
    final Schema schemaA = new Schema();
    schemaA.addField("bool", DataType.BOOL);
    schemaA.addField("int", DataType.INT);
    schemaA.addField("float", DataType.FLOAT);

    final Schema schemaB = new Schema();
    schemaB.addField("bool", DataType.INT);
    schemaB.addField("int", DataType.STRING);
    schemaB.addField("float", DataType.INT);

    final int NROWS = 10;

    final EntityRowsWriter rowsA = new EntityRowsWriter(schemaA);
    for (int i = 1; i <= NROWS; ++i) {
      final FieldFormatWriter writer = rowsA.newRow();
      writer.writeInt(schemaA.fieldIdByName("int"), i * 100);
      writer.writeBool(schemaA.fieldIdByName("bool"), (i & 1) == 1);
      writer.writeFloat(schemaA.fieldIdByName("float"), i * 5.2);
      rowsA.closeRow();
    }
    rowsA.close();

    final byte[] rowsB = rewrite(schemaA, rowsA.toByteArray(), schemaB, NROWS);

    int count = 0;
    final EntityRowsReader rdRowsB = EntityRowsReader.newReader(new ByteArraySlice(rowsB));
    for (int i = 1; rdRowsB.hasMore(); ++i) {
      final FieldFormatReader reader = rdRowsB.next();
      Assertions.assertFalse(reader.isNull(schemaB.fieldIdByName("bool")));
      Assertions.assertEquals((i & 1), reader.getInt(schemaB.fieldIdByName("bool")));
      Assertions.assertFalse(reader.isNull(schemaB.fieldIdByName("int")));
      Assertions.assertEquals(String.valueOf(i * 100), reader.getString(schemaB.fieldIdByName("int")));
      Assertions.assertFalse(reader.isNull(schemaB.fieldIdByName("float")));
      Assertions.assertEquals(Math.round(i * 5.2), reader.getInt(schemaB.fieldIdByName("float")));
      count++;
    }
    Assertions.assertEquals(NROWS, count);
  }


  public static byte[] rewrite(final Schema schemaA, final byte[] rowsA, final Schema schemaB, final int expectedCount) {
    final EntityRowsWriter rowsB = new EntityRowsWriter(schemaB);
    final EntityRowsReader rdRows = EntityRowsReader.newReader(new ByteArraySlice(rowsA));
    final EntityRowConverter converter = rowsB.newRowConverter(schemaA);
    int count = 0;
    while (rdRows.hasMore()) {
      final FieldFormatReader reader = rdRows.next();
      rowsB.addRow(converter, reader);
      count++;
    }
    rowsB.close();
    Assertions.assertEquals(expectedCount, count);
    return rowsB.toByteArray();
  }
}
