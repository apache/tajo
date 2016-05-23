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

package org.apache.tajo.cli.tools;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.JavaResourceUtil;
import org.junit.Test;

import java.util.TimeZone;

import static org.junit.Assert.*;


public class TestDDLBuilder {
  private static final Schema schema1;
  private static final TableMeta meta1;
  private static final PartitionMethodDesc partitionMethod1;
  private static final TajoConf conf;

  static {
    schema1 = SchemaBuilder.builder()
        .add("name", TajoDataTypes.Type.BLOB)
        .add("addr", TajoDataTypes.Type.TEXT)
        .build();

    conf = new TajoConf();
    conf.setSystemTimezone(TimeZone.getTimeZone("Asia/Seoul"));
    meta1 = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, conf);
    meta1.putProperty(StorageConstants.COMPRESSION_CODEC, GzipCodec.class.getName());

    Schema expressionSchema = SchemaBuilder.builder()
        .add("key", TajoDataTypes.Type.INT4)
        .add("key2", TajoDataTypes.Type.TEXT)
        .build();
    partitionMethod1 = new PartitionMethodDesc(
        "db1",
        "table1",
        CatalogProtos.PartitionType.COLUMN,
        "key,key2",
        expressionSchema);
  }

  @Test
  public void testBuildDDLForExternalTable() throws Exception {
    TableDesc desc = new TableDesc("db1.table1", schema1, meta1, new Path("/table1").toUri());
    desc.setPartitionMethod(partitionMethod1);
    desc.setExternal(true);
    assertEquals(JavaResourceUtil.readTextFromResource("results/testDDLBuilder/testBuildDDLForExternalTable.result"),
        DDLBuilder.buildDDLForExternalTable(desc));
  }

  @Test
  public void testBuildDDLQuotedTableName() throws Exception {
    Schema schema2 = SchemaBuilder.builder()
        .add("name", TajoDataTypes.Type.BLOB)
        .add("addr", TajoDataTypes.Type.TEXT)
        .add("FirstName", TajoDataTypes.Type.TEXT)
        .add("LastName", TajoDataTypes.Type.TEXT)
        .add("with", TajoDataTypes.Type.TEXT)
        .build();

    Schema expressionSchema2 = SchemaBuilder.builder()
        .add("BirthYear", TajoDataTypes.Type.INT4)
        .build();

    PartitionMethodDesc partitionMethod2 = new PartitionMethodDesc(
        "db1",
        "table1",
        CatalogProtos.PartitionType.COLUMN,
        "key,key2",
        expressionSchema2);

    TableDesc desc = new TableDesc("db1.TABLE2", schema2, meta1, new Path("/table1").toUri());
    desc.setPartitionMethod(partitionMethod2);
    desc.setExternal(true);
    assertEquals(JavaResourceUtil.readTextFromResource("results/testDDLBuilder/testBuildDDLQuotedTableName1.result"),
        DDLBuilder.buildDDLForExternalTable(desc));

    desc = new TableDesc("db1.TABLE1", schema2, meta1, new Path("/table1").toUri());
    desc.setPartitionMethod(partitionMethod2);
    desc.setExternal(false);
    assertEquals(JavaResourceUtil.readTextFromResource("results/testDDLBuilder/testBuildDDLQuotedTableName2.result"),
        DDLBuilder.buildDDLForBaseTable(desc));
  }

  @Test
  public void testBuildDDLForBaseTable() throws Exception {
    TableDesc desc = new TableDesc("db1.table2", schema1, meta1, new Path("/table1").toUri());
    assertEquals(JavaResourceUtil.readTextFromResource("results/testDDLBuilder/testBuildDDLForBaseTable.result"),
        DDLBuilder.buildDDLForBaseTable(desc));
  }

  @Test
  public void testBuildColumn() throws Exception {
    String [] tobeUnquoted = {
        "column_name",
        "columnname",
        "column_1",
    };

    for (String columnName : tobeUnquoted) {
      assertFalse(IdentifierUtil.isShouldBeQuoted(columnName));
    }

    String [] quoted = {
        "Column_Name",
        "COLUMN_NAME",
        "컬럼",
        "$column_name",
        "Column_Name1",
        "with",
        "when"
    };

    for (String columnName : quoted) {
      assertTrue(IdentifierUtil.isShouldBeQuoted(columnName));
    }
  }
}
