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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.engine.utils.test.ErrorInjectionRewriter;
import org.apache.tajo.jdbc.TajoResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestSelectQuery extends QueryTestCaseBase {

  public TestSelectQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
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
  public final void testExplainSelect() throws Exception {
    // explain select l_orderkey, l_partkey from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
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
    if (!cluster.isHCatalogStoreRunning()) {
      assertEquals(5, orderKeys.getStats().getNumRows().intValue());
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
    if (!testingCluster.isHCatalogStoreRunning()) {
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

  @Test
  public final void testQueryMasterTaskInitError() throws Exception {
    // In this testcase we can check that a TajoClient receives QueryMasterTask's init error message.
    testingCluster.setAllWorkersConfValue("tajo.plan.rewriter.classes",
        ErrorInjectionRewriter.class.getCanonicalName());

    try {
      // If client can't receive error status, thread runs forever.
      Thread t = new Thread() {
        public void run() {
          try {
            TajoResultSet res = (TajoResultSet) client.executeQueryAndGetResult("select l_orderkey from lineitem");
            QueryStatus status = client.getQueryStatus(res.getQueryId());
            assertEquals(QueryState.QUERY_ERROR, status.getState());
            assertEquals(NullPointerException.class.getName(), status.getErrorMessage());
            cleanupQuery(res);
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
      testingCluster.setAllWorkersConfValue("tajo.plan.rewriter.classes", "");
    }
  }
}