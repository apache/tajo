/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.parquet;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.junit.Test;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TajoSchemaConverter}.
 */
public class TestSchemaConverter {
  private static final String ALL_PARQUET_SCHEMA =
      "message table_schema {\n" +
      "  optional boolean myboolean;\n" +
      "  optional int32 myint;\n" +
      "  optional int64 mylong;\n" +
      "  optional float myfloat;\n" +
      "  optional double mydouble;\n" +
      "  optional binary mybytes;\n" +
      "  optional binary mystring (UTF8);\n" +
      "  optional fixed_len_byte_array(1) myfixed;\n" +
      "}\n";

  private static final String CONVERTED_ALL_PARQUET_SCHEMA =
      "message table_schema {\n" +
      "  optional boolean myboolean;\n" +
      "  optional int32 mybit;\n" +
      "  optional binary mychar (UTF8);\n" +
      "  optional int32 myint2;\n" +
      "  optional int32 myint4;\n" +
      "  optional int64 myint8;\n" +
      "  optional float myfloat4;\n" +
      "  optional double myfloat8;\n" +
      "  optional binary mytext (UTF8);\n" +
      "  optional binary myblob;\n" +
      // NULL_TYPE fields are not encoded.
      "  optional binary myinet4;\n" +
      "  optional binary myprotobuf;\n" +
      "}\n";

  private Schema createAllTypesSchema() {
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("myboolean", Type.BOOLEAN));
    columns.add(new Column("mybit", Type.BIT));
    columns.add(new Column("mychar", Type.CHAR));
    columns.add(new Column("myint2", Type.INT2));
    columns.add(new Column("myint4", Type.INT4));
    columns.add(new Column("myint8", Type.INT8));
    columns.add(new Column("myfloat4", Type.FLOAT4));
    columns.add(new Column("myfloat8", Type.FLOAT8));
    columns.add(new Column("mytext", Type.TEXT));
    columns.add(new Column("myblob", Type.BLOB));
    columns.add(new Column("mynull", Type.NULL_TYPE));
    columns.add(new Column("myinet4", Type.INET4));
    columns.add(new Column("myprotobuf", Type.PROTOBUF));
    Column[] columnsArray = new Column[columns.size()];
    columnsArray = columns.toArray(columnsArray);
    return new Schema(columnsArray);
  }

  private Schema createAllTypesConvertedSchema() {
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("myboolean", Type.BOOLEAN));
    columns.add(new Column("myint", Type.INT4));
    columns.add(new Column("mylong", Type.INT8));
    columns.add(new Column("myfloat", Type.FLOAT4));
    columns.add(new Column("mydouble", Type.FLOAT8));
    columns.add(new Column("mybytes", Type.BLOB));
    columns.add(new Column("mystring", Type.TEXT));
    columns.add(new Column("myfixed", Type.BLOB));
    Column[] columnsArray = new Column[columns.size()];
    columnsArray = columns.toArray(columnsArray);
    return new Schema(columnsArray);
  }

  private void testTajoToParquetConversion(
      Schema tajoSchema, String schemaString) throws Exception {
    TajoSchemaConverter converter = new TajoSchemaConverter();
    MessageType schema = converter.convert(tajoSchema);
    MessageType expected = MessageTypeParser.parseMessageType(schemaString);
    assertEquals("converting " + schema + " to " + schemaString,
                 expected.toString(), schema.toString());
  }

  private void testParquetToTajoConversion(
      Schema tajoSchema, String schemaString) throws Exception {
    TajoSchemaConverter converter = new TajoSchemaConverter();
    Schema schema = converter.convert(
        MessageTypeParser.parseMessageType(schemaString));
    assertEquals("converting " + schemaString + " to " + tajoSchema,
                 tajoSchema.toString(), schema.toString());
  }

  @Test
  public void testAllTypesToParquet() throws Exception {
    Schema schema = createAllTypesSchema();
    testTajoToParquetConversion(schema, CONVERTED_ALL_PARQUET_SCHEMA);
  }

  @Test
  public void testAllTypesToTajo() throws Exception {
    Schema schema = createAllTypesConvertedSchema();
    testParquetToTajoConversion(schema, ALL_PARQUET_SCHEMA);
  }
}
