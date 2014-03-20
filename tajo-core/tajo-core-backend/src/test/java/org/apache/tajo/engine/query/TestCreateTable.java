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

import java.util.List;

@Category(IntegrationTest.class)
public class TestCreateTable extends QueryTestCaseBase {

  @Test
  public final void testVariousTypes() throws Exception {
    List<String> createdNames;
    if (testingCluster.isHCatalogStoreRunning()) {
      createdNames = executeDDL("create_table_various_types_for_hcatalog.sql", null);
    } else {
      createdNames = executeDDL("create_table_various_types.sql", null);
    }
    assertTableExists(createdNames.get(0));
  }

  @Test
  public final void testCreateTable1() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1", "table1");
    assertTableExists(createdNames.get(0));
  }

  @Test
  public final void testCreateTable2() throws Exception {
    executeString("CREATE DATABASE D1;").close();
    executeString("CREATE DATABASE D2;").close();

    executeString("CREATE TABLE D1.table1 (age int);").close();
    executeString("CREATE TABLE D1.table2 (age int);").close();
    executeString("CREATE TABLE D2.table3 (age int);").close();
    executeString("CREATE TABLE D2.table4 (age int);").close();

    assertTableExists("D1.table1");
    assertTableExists("D1.table2");
    assertTableNotExists("D2.table1");
    assertTableNotExists("D2.table2");

    assertTableExists("D2.table3");
    assertTableExists("D2.table4");
    assertTableNotExists("D1.table3");
    assertTableNotExists("D1.table4");

    executeString("DROP TABLE D1.table1");
    executeString("DROP TABLE D1.table2");
    executeString("DROP TABLE D2.table3");
    executeString("DROP TABLE D2.table4");

    assertDatabaseExists("D1");
    assertDatabaseExists("D2");
    executeString("DROP DATABASE D1").close();
    executeString("DROP DATABASE D2").close();
    assertDatabaseNotExists("D1");
    assertDatabaseNotExists("D2");
  }

  @Test
  public final void testCreateTableIfNotExists() throws Exception {
    executeString("CREATE DATABASE D3;").close();

    assertTableNotExists("D3.table1");
    executeString("CREATE TABLE D3.table1 (age int);").close();
    assertTableExists("D3.table1");

    executeString("CREATE TABLE IF NOT EXISTS D3.table1 (age int);").close();
    assertTableExists("D3.table1");

    executeString("DROP TABLE D3.table1");
  }

  @Test
  public final void testDropTableIfExists() throws Exception {
    executeString("CREATE DATABASE D4;").close();

    assertTableNotExists("D4.table1");
    executeString("CREATE TABLE D4.table1 (age int);").close();
    assertTableExists("D4.table1");

    executeString("DROP TABLE D4.table1;").close();
    assertTableNotExists("D4.table1");

    executeString("DROP TABLE IF EXISTS D4.table1");
    assertTableNotExists("D4.table1");
  }

  @Test
  public final void testNonreservedKeywordTableNames() throws Exception {
    List<String> createdNames = null;
    createdNames = executeDDL("table1_ddl.sql", "table1", "filter");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "first");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "format");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "grouping");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "hash");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "index");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "insert");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "last");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "location");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "max");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "min");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "national");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "nullif");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "overwrite");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "precision");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "range");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "regexp");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "rlike");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "set");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "unknown");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "var_pop");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "var_samp");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "varying");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "zone");
    assertTableExists(createdNames.get(0));

    createdNames = executeDDL("table1_ddl.sql", "table1", "bigint");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "bit");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "blob");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "bool");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "boolean");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "bytea");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "char");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "date");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "decimal");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "double");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "float");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "float4");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "float8");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "inet4");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int1");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int2");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int4");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int8");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "integer");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "nchar");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "numeric");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "nvarchar");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "real");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "smallint");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "text");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "time");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "timestamp");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "timestamptz");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "timetz");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "tinyint");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "varbinary");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "varbit");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "varchar");
    assertTableExists(createdNames.get(0));
  }
}
