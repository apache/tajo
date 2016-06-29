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

import com.google.common.collect.Lists;
import org.apache.tajo.*;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.plan.rewrite.BaseLogicalPlanRewriteRuleProvider;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.util.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestSelectQuery extends QueryTestCaseBase {

  public TestSelectQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testPositives() throws Exception {
    runPositiveTests();
  }

  @Test
  public final void testNegatives() throws Exception {
    runNegativeTests();
  }

  @Test
  public final void testNonQualifiedNames() throws Exception {
    // select l_orderkey, l_partkey from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNonFromSelect1() throws Exception {
    // select upper('abc');
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSimpleQuery() throws Exception {
    // select * from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSimpleQueryWithLimit() throws Exception {
    // select * from lineitem limit 3;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSimpleQueryWithLimitPartitionedTable() throws Exception {
    // select * from customer_parts limit 10;
    executeDDL("customer_ddl.sql", null);
    for (int i = 0; i < 5; i++) {
      executeFile("insert_into_customer.sql").close();
    }

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE customer_parts PURGE").close();
  }

  @Test
  public final void testExplainSelect() throws Exception {
    // explain select l_orderkey, l_partkey from lineitem;
    testingCluster.getConfiguration().set(ConfVars.$TEST_PLAN_SHAPE_FIX_ENABLED.varname, "true");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      testingCluster.getConfiguration().set(ConfVars.$TEST_PLAN_SHAPE_FIX_ENABLED.varname, "false");
    }
  }

  @Test
  @SimpleTest(queries = {
      @QuerySpec("explain global select l_orderkey, l_partkey from lineitem"),
      @QuerySpec("explain global select n1.n_nationkey, n1.n_name, n2.n_name from nation n1 join nation n2 " +
          "on n1.n_name = upper(n2.n_name) order by n1.n_nationkey"),
      @QuerySpec("explain global select l_linenumber, count(*), count(distinct l_orderkey), sum(distinct l_orderkey) from lineitem " +
          "group by l_linenumber having sum(distinct l_orderkey) = 6")})
  public final void testExplainSelectPhysical() throws Exception {
    runSimpleTests();
  }

  @Test
  public final void testSelect() throws Exception {
    // select l_orderkey, l_partkey from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelect2() throws Exception {
    // select l_orderkey, l_partkey, l_orderkey + l_partkey as plus from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelect3() throws Exception {
    // select l_orderkey + l_partkey as plus from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectColumnAlias1() throws Exception {
    // select l_orderkey as col1, l_orderkey + 1 as col2 from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectColumnAliasExistingInRelation1() throws Exception {
    // We intend that 'l_orderkey' in where clause points to "default.lineitem.l_orderkey"
    // select (l_orderkey + l_orderkey) l_orderkey from lineitem where l_orderkey > 2;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectColumnAliasExistingInRelation2() throws Exception {
    // We intend that 'l_orderkey' in orderby clause points to (-l_orderkey).
    // select (-l_orderkey) as l_orderkey from lineitem order by l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectColumnAliasExistingInRelation3() throws Exception {
    // This is a reproduction code and validator of TAJO-975 Bug
    // Please see TAJO-975 in order to know this test in detail.
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }


  @Test
  public final void testSelectSameConstantsWithDifferentAliases() throws Exception {
    // select l_orderkey, '20130819' as date1, '20130819' as date2 from lineitem where l_orderkey > -1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectSameConstantsWithDifferentAliases2() throws Exception {
    // select l_orderkey, '20130819' as date1, '20130819' as date2, '20130819' as date3, '20130819' as date4
    // from lineitem where l_orderkey > -1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectSameConstantsWithDifferentAliases3() throws Exception {
    // select l_orderkey, '20130819' as date1, '20130819', '20130819', '20130819'
    // from lineitem where l_orderkey > -1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectSameExprsWithDifferentAliases() throws Exception {
    // select l_orderkey, l_partkey + 1 as plus1, l_partkey + 1 as plus2 from lineitem where l_orderkey > -1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereCond1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereCond2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereCondWithAlias1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereCondWithAlias2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk1() throws Exception {
    // select * from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk2() throws Exception {
    // select * from lineitem where l_orderkey = 2;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk3() throws Exception {
    // select * from lineitem where l_orderkey % 2 = 0;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk4() throws Exception {
    // select length(l_comment), l_extendedprice * l_discount, *, l_tax * 10 from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk5() throws Exception {
    // select * from (select l_orderkey, 1 from lineitem where l_orderkey % 2 = 0) t1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectDistinct() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLikeClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testStringCompare() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRealValueCompare() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testCaseWhen() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testCaseWhenWithoutElse() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNotEqual() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInStrClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNotInStrClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNotInClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testCreateAfterSelect() throws Exception {
    ResultSet res = testBase.execute(
        "create table orderkeys as select l_orderkey from lineitem");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, "orderkeys"));
    TableDesc orderKeys = catalog.getTableDesc(DEFAULT_DATABASE_NAME, "orderkeys");
    if (!cluster.isHiveCatalogStoreRunning()) {
      assertEquals(8, orderKeys.getStats().getNumRows().intValue());
    }
  }

  @Test
  public final void testLimit() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectWithJson() throws Exception {
    // select l_orderkey, l_partkey + 1 as plus1, l_partkey + 1 as plus2 from lineitem where l_orderkey > -1;
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDatabaseRef() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      executeString("CREATE DATABASE \"TestSelectQuery\"").close();
      executeString("CREATE TABLE \"TestSelectQuery\".\"LineItem\" AS SELECT * FROM default.lineitem" ).close();

      ResultSet res = executeFile("testDatabaseRef1.sql");
      assertResultSet(res, "testDatabaseRef.result");
      cleanupQuery(res);

      res = executeFile("testDatabaseRef2.sql");
      assertResultSet(res, "testDatabaseRef.result");
      cleanupQuery(res);

      res = executeFile("testDatabaseRef3.sql");
      assertResultSet(res, "testDatabaseRef.result");
      cleanupQuery(res);

      executeString("DROP DATABASE \"TestSelectQuery\"").close();
    }
  }

  @Test
  public final void testSumIntOverflow() throws Exception {
    // Test data's min value is 17 and number of rows is 5.
    // 25264513 = 2147483647/17/5
    // result is 116,848,374,845 ==> int overflow
    // select sum(cast(l_quantity * 25264513 as INT4)) from lineitem where l_quantity > 0;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSumFloatOverflow() throws Exception {
    // Test data's min value is 21168.23 and number of rows is 5.
    // 3.21506374375027E33 = 3.40282346638529E38/21168/ 5
    // result is 6.838452478692677E38 ==> float4 overflow
    // select sum(cast(L_EXTENDEDPRICE * 3.21506374375027E33 as FLOAT4)) from lineitem where l_quantity > 0;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  public static class RulesForErrorInjection extends BaseLogicalPlanRewriteRuleProvider {
    public RulesForErrorInjection(TajoConf conf) {
      super(conf);
    }

    @Override
    public Collection<Class<? extends LogicalPlanRewriteRule>> getPostRules() {
      List<Class<? extends LogicalPlanRewriteRule>> addedRules = Lists.newArrayList(super.getPostRules());
      return addedRules;
    }
  }

  @Test
  public final void testQueryMasterTaskInitError() throws Exception {
    // In this testcase we can check that a TajoClient receives QueryMasterTask's init error message.
    testingCluster.setAllWorkersConfValue(ConfVars.LOGICAL_PLAN_REWRITE_RULE_PROVIDER_CLASS.name(),
        RulesForErrorInjection.class.getCanonicalName());

    try {
      // If client can't receive error status, thread runs forever.
      Thread t = new Thread() {
        public void run() {
          try {
            ClientProtos.SubmitQueryResponse response = client.executeQuery("select l_orderkey from lineitem");
            QueryStatus status = client.getQueryStatus(new QueryId(response.getQueryId()));
            assertEquals(QueryState.QUERY_ERROR, status.getState());
            assertEquals(NullPointerException.class.getName(), status.getErrorMessage());
          } catch (Exception e) {
            fail(e.getMessage());
          }
        }
      };

      t.start();

      for (int i = 0; i < 10; i++) {
        Thread.sleep(1 * 1000);
        if (!t.isAlive()) {
          break;
        }
      }

      // If query runs more than 10 secs, test is fail.
      assertFalse(t.isAlive());
    } finally {
      // recover the rewrite rule provider to default
      testingCluster.setAllWorkersConfValue(ConfVars.LOGICAL_PLAN_REWRITE_RULE_PROVIDER_CLASS.name(), "");
    }
  }

  @Test
  public final void testNowInMultipleTasks() throws Exception {
    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("name", Type.TEXT)
        .build();
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3", "4|table11-4", "5|table11-5" };
    TajoTestingCluster.createTable(conf, "testNowInMultipleTasks".toLowerCase(), schema, data, 2);

    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "2");

      ResultSet res = executeString("select concat(substr(to_char(now(),'yyyymmddhh24miss'), 1, 14), 'aaa'), sleep(1) " +
          "from testNowInMultipleTasks");

      String nowValue = null;
      int numRecords = 0;
      while (res.next()) {
        String currentNowValue = res.getString(1);
        if (nowValue != null) {
          assertTrue(nowValue + " is different to " + currentNowValue, nowValue.equals(currentNowValue));
        }
        nowValue = currentNowValue;
        numRecords++;
      }
      assertEquals(5, numRecords);

      res.close();

      res = executeString("select concat(substr(to_char(current_timestamp,'yyyymmddhh24miss'), 1, 14), 'aaa'), sleep(1) " +
          "from testNowInMultipleTasks");

      nowValue = null;
      numRecords = 0;
      while (res.next()) {
        String currentNowValue = res.getString(1);
        if (nowValue != null) {
          assertTrue(nowValue.equals(currentNowValue));
        }
        nowValue = currentNowValue;
        numRecords++;
      }
      assertEquals(5, numRecords);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname,
          ConfVars.$TEST_MIN_TASK_NUM.defaultVal);
      executeString("DROP TABLE testNowInMultipleTasks PURGE");
    }
  }

  @Test
  public void testCaseWhenRound() throws Exception {
    /*
    select *
        from (select n_nationkey as key,
    case
      when n_nationkey > 6 then round((n_nationkey * 100 / 2.123) / (n_regionkey * 50 / 2.123), 2) else 100.0 end as val
    from
      nation
    where
      n_regionkey > 0 and n_nationkey > 0
    ) a
    order by
      a.key
    */

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testColumnEqualityButNotJoinCondition1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testColumnEqualityButNotJoinCondition2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testTimezonedTable1() throws Exception {
    // Table - GMT (No table property or no system timezone)
    // Client - GMT (default client time zone is used if no TIME ZONE session variable is given.)
    try {
      executeDDL("datetime_table_ddl.sql", "timezoned", new String[]{"timezoned1"});
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned1");
    }
  }

  @Test
  public void testTimezonedTable2() throws Exception {
    // Table - timezone = GMT+9
    // Client - GMT (SET TIME ZONE 'GMT';)
    try {
      executeDDL("datetime_table_timezoned_ddl.sql", "timezoned", new String[]{"timezoned2"});
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned2");
    }
  }

  @Test
  public void testTimezonedTable3() throws Exception {
    // Table - timezone = GMT+9
    // Client - GMT+9 through TajoClient API

    Map<String,String> sessionVars = new HashMap<>();
    sessionVars.put(SessionVars.TIMEZONE.name(), "GMT+9");
    getClient().updateSessionVariables(sessionVars);

    try {
      executeDDL("datetime_table_timezoned_ddl.sql", "timezoned", new String[]{"timezoned3"});
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned3");
    }
  }

  @Test
  public void testTimezonedTable4() throws Exception {
    // Table - timezone = GMT+9
    // Client - GMT+9 (SET TIME ZONE 'GMT+9';)

    try {
      executeDDL("datetime_table_timezoned_ddl.sql", "timezoned", new String[]{"timezoned4"});
      ResultSet res = executeQuery();
      assertResultSet(res, "testTimezonedTable3.result");
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned4");
    }
  }

  @Test
  public void testTimezonedTable5() throws Exception {
    // Table - timezone = GMT+9 (by a specified system timezone)
    // Client - GMT+9 (SET TIME ZONE 'GMT+9';)

    TimeZone systemTimeZone = testingCluster.getConfiguration().getSystemTimezone();
    try {
      testingCluster.getConfiguration().setSystemTimezone(TimeZone.getTimeZone("GMT+9"));

      executeDDL("datetime_table_ddl.sql", "timezoned", new String[]{"timezoned5"});
      ResultSet res = executeQuery();
      assertResultSet(res, "testTimezonedTable3.result");
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned5");

      // restore the config
      testingCluster.getConfiguration().setSystemTimezone(systemTimeZone);
    }
  }

  @Test
  public void testLoadIntoTimezonedTable() throws Exception {
    // Insert from timezoned table into another timezoned table

    try {
      executeDDL("datetime_table_timezoned_ddl.sql", "timezoned", "timezoned_load1");
      executeDDL("datetime_table_timezoned_ddl2.sql", null, "timezoned_load2");
      executeString("INSERT OVERWRITE INTO timezoned_load2 SELECT * FROM timezoned_load1");

      ResultSet res = executeQuery();
      assertResultSet(res, "testTimezonedTable3.result");
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned_load1");
      executeString("DROP TABLE IF EXISTS timezoned_load2 PURGE");
    }
  }
  
  @Test
  public void testMultiBytesDelimiter1() throws Exception {
    executeDDL("multibytes_delimiter_table1_ddl.sql", "multibytes_delimiter1");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table1");
    }
  }
  
  @Test
  public void testMultiBytesDelimiter2() throws Exception {
    executeDDL("multibytes_delimiter_table2_ddl.sql", "multibytes_delimiter2");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table2");
    }
  }

  @Test
  public void testMultiBytesDelimiter3() throws Exception {
    executeDDL("multibytes_delimiter_table3_ddl.sql", "multibytes_delimiter1");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table1");
    }
  }

  @Test
  public void testMultiBytesDelimiter4() throws Exception {
    executeDDL("multibytes_delimiter_table4_ddl.sql", "multibytes_delimiter2");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table2");
    }
  }

  @Test
  public void testSimpleQueryWithPythonFuncs() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testSelectPythonFuncs() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testSelectWithPredicateOnPythonFunc() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testNestedPythonFunction() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testSelectWithParentheses1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testSelectWithParentheses2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testSelectOnSessionTable() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}