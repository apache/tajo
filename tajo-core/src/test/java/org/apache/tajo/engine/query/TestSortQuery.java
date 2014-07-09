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
import org.apache.tajo.conf.TajoConf;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.util.TimeZone;

@Category(IntegrationTest.class)
public class TestSortQuery extends QueryTestCaseBase {

  public TestSortQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testSort() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAlias1() throws Exception {
    // select l_linenumber, l_orderkey as sortkey from lineitem order by sortkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAlias2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAlias3() throws Exception {
    ResultSet res = executeQuery();
    System.out.println(resultSetToString(res));
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithExpr1() throws Exception {
    // select l_linenumber, l_orderkey as sortkey from lineitem order by l_orderkey + 1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithExpr2() throws Exception {
    // select l_linenumber, l_orderkey as sortkey from lineitem order by l_linenumber, l_orderkey, (l_orderkey is null);
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAliasButOriginalName() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortDesc() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTopK() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortAfterGroupby() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortAfterGroupbyWithAlias() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithDate() throws Exception {
    // skip this test if catalog uses HCatalogStore.
    // It is because HCatalogStore does not support Time data type.
    TimeZone oldTimeZone = TajoConf.setCurrentTimeZone(TimeZone.getTimeZone("UTC"));
    TimeZone systemOldTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    try {
      if (!testingCluster.isHCatalogStoreRunning()) {
        // create external table table1 (col1 timestamp, col2 date, col3 time) ...
        executeDDL("create_table_with_date_ddl.sql", "table1");

        ResultSet res = executeQuery();
        assertResultSet(res);
        cleanupQuery(res);
      }
    } finally {
      TajoConf.setCurrentTimeZone(oldTimeZone);
      TimeZone.setDefault(systemOldTimeZone);
    }
  }

  @Test
  public final void testAsterisk() throws Exception {
    //select *, length(l_comment) as len_comment from lineitem order by len_comment;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAscDescKeys() throws Exception {
    executeDDL("create_table_with_asc_desc_keys.sql", "table2");

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithJson() throws Exception {
    // select max(l_quantity) as max_quantity, l_orderkey from lineitem group by l_orderkey order by max_quantity;
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTopkWithJson() throws Exception {
    // select l_orderkey, l_linenumber from lineitem order by l_orderkey desc limit 3;
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
