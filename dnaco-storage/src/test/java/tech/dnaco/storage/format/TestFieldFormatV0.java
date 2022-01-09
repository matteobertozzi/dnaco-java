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

package tech.dnaco.storage.format;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.geo.LatLong;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.storage.Schema;

public class TestFieldFormatV0 {
  @Test
  public void testNullFields() {
    final Schema schema = new Schema();
    schema.addField("bool", DataType.BOOL);
    schema.addField("int", DataType.INT);
    schema.addField("float", DataType.FLOAT);

    final PagedByteArray buffer = new PagedByteArray(1 << 10);
    try (FieldFormatWriter writer = FieldFormat.newWriter(schema, buffer, 0)) {
      writer.writeNull(schema.fieldIdByName("int"));
    }

    final FieldFormatReader reader = FieldFormat.newReader(schema, 0);
    reader.load(new ByteArraySlice(buffer.toByteArray()));
    Assertions.assertTrue(reader.isNull(schema.fieldIdByName("bool")));
    Assertions.assertTrue(reader.isNull(schema.fieldIdByName("int")));
    Assertions.assertTrue(reader.isNull(schema.fieldIdByName("float")));
  }

  @Test
  public void testWriteAndRead() {
    final Schema schema = new Schema();
    schema.addField("bool", DataType.BOOL);
    schema.addField("int", DataType.INT);
    schema.addField("float", DataType.FLOAT);
    schema.addField("bytes", DataType.BYTES);
    schema.addField("string", DataType.STRING);
    schema.addField("array", DataType.ARRAY);
    schema.addField("object", DataType.OBJECT);
    schema.addField("timestamp", DataType.UTC_TIMESTAMP);
    schema.addField("geo_location", DataType.GEO_LOCATION);
    schema.addField("geo_line", DataType.GEO_LINE);
    schema.addField("geo_polygon", DataType.GEO_POLYGON);

    for (int i = 1; i <= 500; i += 7) {
      final boolean boolValue = (i & 1) == 0;
      final long intValue = i * 100L;
      final double floatValue = i * 10.25;
      final byte[] bytesValue = new byte[] { (byte) (i & 0xff), (byte) ((i + 1) & 0xff) };
      final String stringValue = "hello-" + i;
      final JsonArray arrayValue = new JsonArray().add(1).add("foo").add(3);
      final JsonObject objectValue = new JsonObject().add("x", 10).add("b", "foo");

      final PagedByteArray buffer = new PagedByteArray(1 << 20);
      try (FieldFormatWriter writer = FieldFormat.newWriter(schema, buffer, 0)) {
        writer.writeBool(schema.fieldIdByName("bool"), boolValue);
        writer.writeInt(schema.fieldIdByName("int"), intValue);
        writer.writeFloat(schema.fieldIdByName("float"), floatValue);
        writer.writeBytes(schema.fieldIdByName("bytes"), bytesValue);
        writer.writeString(schema.fieldIdByName("string"), stringValue);
        writer.writeArray(schema.fieldIdByName("array"), arrayValue);
        writer.writeObject(schema.fieldIdByName("object"), objectValue);
        writer.writeGeoLocation(schema.fieldIdByName("geo_location"), 51.5320754, -0.1802646);
      }

      final FieldFormatReader reader = FieldFormat.newReader(schema, 0);
      reader.load(new ByteArraySlice(buffer.toByteArray()));
      Assertions.assertFalse(reader.isNull(schema.fieldIdByName("bool")));
      Assertions.assertEquals(boolValue, reader.getBool(schema.fieldIdByName("bool")));
      Assertions.assertFalse(reader.isNull(schema.fieldIdByName("int")));
      Assertions.assertEquals(intValue, reader.getInt(schema.fieldIdByName("int")));
      Assertions.assertEquals(intValue, reader.getFloat(schema.fieldIdByName("int")), 0.0001);
      Assertions.assertFalse(reader.isNull(schema.fieldIdByName("float")));
      Assertions.assertEquals(floatValue, reader.getFloat(schema.fieldIdByName("float")), 0.0001);
      Assertions.assertFalse(reader.isNull(schema.fieldIdByName("bytes")));
      Assertions.assertArrayEquals(bytesValue, reader.getBytes(schema.fieldIdByName("bytes")).buffer());
      Assertions.assertFalse(reader.isNull(schema.fieldIdByName("string")));
      Assertions.assertEquals(stringValue, reader.getString(schema.fieldIdByName("string")));
      Assertions.assertFalse(reader.isNull(schema.fieldIdByName("array")));
      Assertions.assertEquals(arrayValue, reader.getArray(schema.fieldIdByName("array")));
      Assertions.assertFalse(reader.isNull(schema.fieldIdByName("object")));
      Assertions.assertEquals(objectValue, reader.getObject(schema.fieldIdByName("object")));

      Assertions.assertFalse(reader.isNull(schema.fieldIdByName("geo_location")));
      final LatLong latlng = reader.getGeoLocation(schema.fieldIdByName("geo_location"));
      Assertions.assertEquals(51.5320754, latlng.getLatitude(),  0.0000001);
      Assertions.assertEquals(-0.1802646, latlng.getLongitude(), 0.0000001);
    }
  }
}
