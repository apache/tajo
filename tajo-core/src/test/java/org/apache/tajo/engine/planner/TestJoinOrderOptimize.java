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

import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.junit.Test;

public class TestJoinOrderOptimize extends QueryPlanTestCaseBase {

  public TestJoinOrderOptimize() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);

    testBase.util.getConf().set(
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testBase.util.getConf().set(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testBase.util.getConf().set(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);

    testBase.util.getConf().set(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "false");
    testBase.util.getConf().set(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname, "-1");
  }

  @Test
  public final void testJoinWithMultipleJoinTypes() throws Exception {
    String plan = executeQuery();
    assertPlan(plan);
  }

  @Test
  public final void testWhereClauseJoin5() throws Exception {
    String plan = executeQuery();
    assertPlan(plan);
  }

  @Test
  public final void testWhereClauseJoin6() throws Exception {
    String plan = executeQuery();
    assertPlan(plan);
  }

  @Test
  public final void testJoinWithMultipleJoinQual1() throws Exception {
    String plan = executeQuery();
    assertPlan(plan);
  }

  @Test
  public final void testJoinWithMultipleJoinQual4() throws Exception {
    String plan = executeQuery();
    assertPlan(plan);
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      String plan = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from default.table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id and t2.id = t3.id");

      assertPlan(plan);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase3() throws Exception {
    createOuterJoinTestTable();
    try {
      String plan = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id "
      );
      assertPlan(plan);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  private void createOuterJoinTestTable() throws Exception {
    String table = "table11";
    String location = TajoPlanTestingUtility.DEFAULT_TEST_DIRECTORY + "/" + table + ".tbl";
    String ddl = String.format("create external table %s (id int4, name text) using text with ('text.delimiter'='|') location '%s'",
        table, location);
    createTable(ddl, location, new String[]{ "1|table11-1", "2|table11-2", "3|table11-3" });

    table = "table12";
    location = TajoPlanTestingUtility.DEFAULT_TEST_DIRECTORY + "/" + table + ".tbl";
    ddl = String.format("create external table %s (id int4, name text) using text with ('text.delimiter'='|') location '%s'",
        table, location);
    createTable(ddl, location, new String[]{ "1|table12-1" });

    table = "table13";
    location = TajoPlanTestingUtility.DEFAULT_TEST_DIRECTORY + "/" + table + ".tbl";
    ddl = String.format("create external table %s (id int4, name text) using text with ('text.delimiter'='|') location '%s'",
        table, location);
    createTable(ddl, location, new String[]{ "2|table13-2", "3|table13-3" });

    table = "table14";
    location = TajoPlanTestingUtility.DEFAULT_TEST_DIRECTORY + "/" + table + ".tbl";
    ddl = String.format("create external table %s (id int4, name text) using text with ('text.delimiter'='|') location '%s'",
        table, location);
    createTable(ddl, location, new String[]{ "1|table14-1", "2|table14-2", "3|table14-3", "4|table14-4" });

  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase2() throws Exception {
    // outer -> outer -> inner
    createOuterJoinTestTable();
    try {
      String plan = executeString(
          "select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t2.id = t3.id\n" +
              "inner join table14 t4\n" +
              "on t2.id = t4.id"
      );
      assertPlan(plan);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  private void dropOuterJoinTestTable() throws Exception {
    executeDDLString("DROP TABLE table11 PURGE;");
    executeDDLString("DROP TABLE table12 PURGE;");
    executeDDLString("DROP TABLE table13 PURGE;");
    executeDDLString("DROP TABLE table14 PURGE;");
  }
}
