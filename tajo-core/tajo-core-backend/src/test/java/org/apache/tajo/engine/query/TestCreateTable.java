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

package org.apache.tajo.engine.query;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TestCreateTable extends QueryTestCaseBase {

  @Test
  public final void testVariousTypes() throws Exception {
    String tableName = executeDDL("create_table_various_types.sql", null);
    assertTableExists(tableName);
  }

  @Test
  public final void testCreateTable1() throws Exception {
    String tableName = executeDDL("table1_ddl.sql", "table1.tbl", "table1");
    assertTableExists(tableName);
  }

  @Test
  public final void testNonreservedKeywordTableNames() throws Exception {
    String tableName = null;
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "filter");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "first");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "format");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "grouping");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "hash");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "index");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "insert");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "last");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "location");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "max");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "min");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "national");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "nullif");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "overwrite");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "precision");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "range");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "regexp");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "rlike");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "set");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "unknown");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "var_pop");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "var_samp");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "varying");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "zone");
    assertTableExists(tableName);

    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "bigint");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "bit");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "blob");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "bool");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "boolean");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "bytea");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "char");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "date");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "decimal");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "double");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "float");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "float4");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "float8");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "inet4");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "int");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "int1");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "int2");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "int4");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "int8");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "integer");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "nchar");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "numeric");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "nvarchar");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "real");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "smallint");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "text");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "time");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "timestamp");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "timestamptz");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "timetz");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "tinyint");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "varbinary");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "varbit");
    assertTableExists(tableName);
    tableName = executeDDL("table1_ddl.sql", "table1.tbl", "varchar");
    assertTableExists(tableName);
  }
}
