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
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestDDLBuilder {
  private static final Schema schema1;
  private static final TableMeta meta1;
  private static final PartitionMethodDesc partitionMethod1;

  static {
    schema1 = new Schema();
    schema1.addColumn("name", TajoDataTypes.Type.BLOB);
    schema1.addColumn("addr", TajoDataTypes.Type.TEXT);

    meta1 = CatalogUtil.newTableMeta("CSV");
    meta1.putOption(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    meta1.putOption(StorageConstants.COMPRESSION_CODEC, GzipCodec.class.getName());

    Schema expressionSchema = new Schema();
    expressionSchema.addColumn("key", TajoDataTypes.Type.INT4);
    expressionSchema.addColumn("key2", TajoDataTypes.Type.TEXT);
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
    assertEquals(FileUtil.readTextFileFromResource("results/testDDLBuilder/testBuildDDLForExternalTable.result"),
        DDLBuilder.buildDDLForExternalTable(desc));
  }

  @Test
  public void testBuildDDLQuotedTableName() throws Exception {
    Schema schema2 = new Schema();
    schema2.addColumn("name", TajoDataTypes.Type.BLOB);
    schema2.addColumn("addr", TajoDataTypes.Type.TEXT);
    schema2.addColumn("FirstName", TajoDataTypes.Type.TEXT);
    schema2.addColumn("LastName", TajoDataTypes.Type.TEXT);
    schema2.addColumn("with", TajoDataTypes.Type.TEXT);

    Schema expressionSchema2 = new Schema();
    expressionSchema2.addColumn("BirthYear", TajoDataTypes.Type.INT4);

    PartitionMethodDesc partitionMethod2 = new PartitionMethodDesc(
        "db1",
        "table1",
        CatalogProtos.PartitionType.COLUMN,
        "key,key2",
        expressionSchema2);

    TableDesc desc = new TableDesc("db1.TABLE2", schema2, meta1, new Path("/table1").toUri());
    desc.setPartitionMethod(partitionMethod2);
    desc.setExternal(true);
    assertEquals(FileUtil.readTextFileFromResource("results/testDDLBuilder/testBuildDDLQuotedTableName1.result"),
        DDLBuilder.buildDDLForExternalTable(desc));

    desc = new TableDesc("db1.TABLE1", schema2, meta1, new Path("/table1").toUri());
    desc.setPartitionMethod(partitionMethod2);
    desc.setExternal(false);
    assertEquals(FileUtil.readTextFileFromResource("results/testDDLBuilder/testBuildDDLQuotedTableName2.result"),
        DDLBuilder.buildDDLForBaseTable(desc));
  }

  @Test
  public void testBuildDDLForBaseTable() throws Exception {
    TableDesc desc = new TableDesc("db1.table2", schema1, meta1, new Path("/table1").toUri());
    assertEquals(FileUtil.readTextFileFromResource("results/testDDLBuilder/testBuildDDLForBaseTable.result"),
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
      assertFalse(CatalogUtil.isShouldBeQuoted(columnName));
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
      assertTrue(CatalogUtil.isShouldBeQuoted(columnName));
    }
  }
}
