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

import java.sql.ResultSet;
import java.util.List;

@Category(IntegrationTest.class)
public class TestAlterTable extends QueryTestCaseBase {
  @Test
  public final void testAlterTableName() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "ABC");
    assertTableExists(createdNames.get(0));
    executeDDL("alter_table_rename_table_ddl.sql", null);
    assertTableExists("DEF");
  }

  @Test
  public final void testAlterTableColumnName() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "XYZ");
    executeDDL("alter_table_rename_column_ddl.sql", null);
    assertColumnExists(createdNames.get(0),"renum");
  }

  @Test
  public final void testAlterTableAddNewColumn() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "EFG");
    executeDDL("alter_table_add_new_column_ddl.sql", null);
    assertColumnExists(createdNames.get(0),"cool");
  }

  @Test
  public final void testAlterTableSetProperty() throws Exception {
    List<String> createdNames = executeDDL("table2_ddl.sql", "table2.tbl", "ALTX");

    ResultSet before_res = executeQuery();
    assertResultSet(before_res, "before_set_property.result");
    cleanupQuery(before_res);

    executeDDL("alter_table_set_property.sql", null);

    ResultSet after_res = executeQuery();
    assertResultSet(after_res, "after_set_property.result");
    cleanupQuery(after_res);
  }
}
