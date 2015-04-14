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

package org.apache.tajo.engine.planner;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;

public class TestJoinOrderOptimize extends QueryTestCaseBase {

  public TestJoinOrderOptimize() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @BeforeClass
  public static void classSetUp() throws Exception {
    testingCluster.getConfiguration().set(TajoConf.ConfVars.$TEST_PLAN_SHAPE_FIX_ENABLED.varname, "true");
  }

  @AfterClass
  public static void classTearDown() {
    testingCluster.getConfiguration().set(TajoConf.ConfVars.$TEST_PLAN_SHAPE_FIX_ENABLED.varname, "false");
  }

  private void createOuterJoinTestTable() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{ "1|table12-1" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"2|table13-2", "3|table13-3" };
    TajoTestingCluster.createTable("table13", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"1|table14-1", "2|table14-2", "3|table14-3", "4|table14-4" };
    TajoTestingCluster.createTable("table14", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{};
    TajoTestingCluster.createTable("table15", schema, tableOptions, data);
  }

  private void dropOuterJoinTestTable() throws Exception {
    executeString("DROP TABLE table11 PURGE;");
    executeString("DROP TABLE table12 PURGE;");
    executeString("DROP TABLE table13 PURGE;");
    executeString("DROP TABLE table14 PURGE;");
    executeString("DROP TABLE table15 PURGE;");
  }

  @Test
  public final void testJoinWithMultipleJoinTypes() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin6() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithMultipleJoinQual1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithMultipleJoinQual4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id\n" +
              "from default.table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id and t2.id = t3.id");

      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase3() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id "
      );
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase2() throws Exception {
    // outer -> outer -> inner
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t2.id = t3.id\n" +
              "inner join table14 t4\n" +
              "on t2.id = t4.id"
      );
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }
}
