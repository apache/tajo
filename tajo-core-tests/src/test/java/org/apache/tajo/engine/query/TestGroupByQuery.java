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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.history.QueryHistory;
import org.apache.tajo.util.history.StageHistory;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.util.*;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class TestGroupByQuery extends QueryTestCaseBase {
  private static final Log LOG = LogFactory.getLog(TestGroupByQuery.class);

  public TestGroupByQuery(String groupByOption) throws Exception {
    super(TajoConstants.DEFAULT_DATABASE_NAME);

    Map<String, String> variables = new HashMap<String, String>();
    if (groupByOption.equals("MultiLevel")) {
      variables.put(SessionVars.GROUPBY_MULTI_LEVEL_ENABLED.keyname(), "true");
    } else {
      variables.put(SessionVars.GROUPBY_MULTI_LEVEL_ENABLED.keyname(), "false");
    }
    client.updateSessionVariables(variables);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.unsetSessionVariables(TUtil.newList(SessionVars.GROUPBY_MULTI_LEVEL_ENABLED.keyname()));
  }

  @Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"MultiLevel"},
        {"No-MultiLevel"},
    });
  }

  @Test
  public final void testGroupBy() throws Exception {
    // select count(1) as unique_key from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupBy2() throws Exception {
    // select count(1) as unique_key from lineitem group by l_linenumber;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupBy3() throws Exception {
    // select l_orderkey as gkey from lineitem group by gkey order by gkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupBy4() throws Exception {
    // select l_orderkey as gkey, count(1) as unique_key from lineitem group by lineitem.l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupBy5() throws Exception {
    // select l_orderkey as gkey, '00' as num from lineitem group by lineitem.l_orderkey order by gkey
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByNested1() throws Exception {
    // select l_orderkey + l_partkey as unique_key from lineitem group by l_orderkey + l_partkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByNested2() throws Exception {
    // select sum(l_orderkey) + sum(l_partkey) as total from lineitem group by l_orderkey + l_partkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithSameExprs1() throws Exception {
    // select sum(l_orderkey) + sum(l_orderkey) as total from lineitem group by l_orderkey + l_partkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithSameExprs2() throws Exception {
    // select sum(l_orderkey) as total1, sum(l_orderkey) as total2 from lineitem group by l_orderkey + l_partkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithExpressionKeys1() throws Exception {
    // select upper(lower(l_orderkey::text)) as key, count(1) as total from lineitem
    // group by key order by upper(lower(l_orderkey::text)), total;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithExpressionKeys2() throws Exception {
    // select upper(lower(l_orderkey::text)) as key, count(1) as total from lineitem
    // group by upper(lower(l_orderkey::text)) order by upper(l_orderkey::text), total;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithSameConstantKeys1() throws Exception {
    // select l_partkey as a, '##' as b, '##' as c, count(*) d from lineitem group by a, b, c order by a;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithConstantKeys1() throws Exception {
    // select 123 as key, count(1) as total from lineitem group by key order by key, total;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithConstantKeys2() throws Exception {
    // select l_partkey as a, timestamp '2014-07-07 04:28:31.561' as b, '##' as c, count(*) d from lineitem
    // group by a, b, c order by l_partkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithConstantKeys3() throws Exception {
    // select
    //   l_partkey as a,
    //   timestamp '2014-07-07 04:28:31.561' as b,
    //   '##' as c,
    //   count(*) d
    // from
    //   lineitem
    // group by
    //  b, c;         <- b and c all are constant values.
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithConstantKeys4() throws Exception {
    //    select
    //    'day',
    //        l_orderkey,
    //        count(*) as sum
    //    from
    //        lineitem
    //    group by
    //    'day',
    //        l_orderkey
    //    order by
    //    'day',
    //        l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithConstantKeys5() throws Exception {
    //    select
    //    'day',
    //        l_orderkey,
    //        count(*) as sum
    //    from
    //        lineitem
    //    group by
    //    'day',
    //        l_orderkey
    //    order by
    //    'day',
    //        l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregation1() throws Exception {
    // select l_orderkey, max(l_orderkey) as maximum, count(distinct l_linenumber) as unique_key from lineitem
    // group by l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * This is an unit test for a combination of aggregation and distinct aggregation functions.
   */
  public final void testDistinctAggregation2() throws Exception {
    // select l_orderkey, count(*) as cnt, count(distinct l_linenumber) as unique_key from lineitem group by l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregation3() throws Exception {
    // select count(*), count(distinct l_orderkey), sum(distinct l_orderkey) from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregation4() throws Exception {
    // select l_linenumber, count(*), count(distinct l_orderkey), sum(distinct l_orderkey)
    // from lineitem group by l_linenumber;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregation5() throws Exception {
    // select sum(distinct l_orderkey), l_linenumber, count(distinct l_orderkey), count(*) as total
    // from lineitem group by l_linenumber;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregation6() throws Exception {
    // select count(distinct l_orderkey) v0, sum(l_orderkey) v1, sum(l_linenumber) v2, count(*) as v4 from lineitem
    // group by l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregation7() throws Exception {
    // select count(*), count(distinct c_nationkey), count(distinct c_mktsegment) from customer
    // tpch scale 1000: 15000000	25	5
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregation8() throws Exception {
    /*
    select
    sum(distinct l_orderkey),
        l_linenumber, l_returnflag, l_linestatus, l_shipdate,
        count(distinct l_partkey),
        sum(l_orderkey)
    from
        lineitem
    group by
    l_linenumber, l_returnflag, l_linestatus, l_shipdate;
    */
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregationWithHaving1() throws Exception {
    // select l_linenumber, count(*), count(distinct l_orderkey), sum(distinct l_orderkey) from lineitem
    // group by l_linenumber having sum(distinct l_orderkey) >= 6;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregationWithUnion1() throws Exception {
    // select sum(distinct l_orderkey), l_linenumber, count(distinct l_orderkey), count(*) as total
    // from (select * from lineitem union select * from lineitem) group by l_linenumber;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDistinctAggregationCasebyCase1() throws Exception {
    // one groupby, distinct, aggregation
    ResultSet res = executeFile("testDistinctAggregation_case1.sql");
    assertResultSet(res, "testDistinctAggregation_case1.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase2() throws Exception {
    // one groupby, two distinct, one aggregation
    ResultSet res = executeFile("testDistinctAggregation_case2.sql");
    assertResultSet(res, "testDistinctAggregation_case2.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase3() throws Exception {
    // one groupby, two distinct, two aggregation(no alias)
    ResultSet res = executeFile("testDistinctAggregation_case3.sql");
    assertResultSet(res, "testDistinctAggregation_case3.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase4() throws Exception {
    // two groupby, two distinct, two aggregation
    ResultSet res = executeFile("testDistinctAggregation_case4.sql");
    assertResultSet(res, "testDistinctAggregation_case4.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase5() throws Exception {
    // two groupby, two distinct, two aggregation with stage
    ResultSet res = executeFile("testDistinctAggregation_case5.sql");
    assertResultSet(res, "testDistinctAggregation_case5.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase6() throws Exception {
    ResultSet res = executeFile("testDistinctAggregation_case6.sql");
    assertResultSet(res, "testDistinctAggregation_case6.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase7() throws Exception {
    ResultSet res = executeFile("testDistinctAggregation_case7.sql");
    assertResultSet(res, "testDistinctAggregation_case7.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase8() throws Exception {
    ResultSet res = executeFile("testDistinctAggregation_case8.sql");
    assertResultSet(res, "testDistinctAggregation_case8.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase9() throws Exception {
    ResultSet res = executeFile("testDistinctAggregation_case9.sql");
    assertResultSet(res, "testDistinctAggregation_case9.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase10() throws Exception {
    ResultSet res = executeFile("testDistinctAggregation_case10.sql");
    assertResultSet(res, "testDistinctAggregation_case10.result");
    res.close();
  }

  @Test
  public final void testDistinctAggregationCasebyCase11() throws Exception {
    ResultSet res;

    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.TEXT);
    schema.addColumn("code", Type.TEXT);
    schema.addColumn("qty", Type.INT4);
    schema.addColumn("qty2", Type.FLOAT8);
    String[] data = new String[]{"1|a|3|3.0", "1|a|4|4.0", "1|b|5|5.0", "2|a|1|6.0", "2|c|2|7.0", "2|d|3|8.0"};
    TajoTestingCluster.createTable("table10", schema, tableOptions, data);

    res = executeString("select id, count(distinct code), " +
        "avg(qty), min(qty), max(qty), sum(qty), " +
        "cast(avg(qty2) as INT8), cast(min(qty2) as INT8), cast(max(qty2) as INT8), cast(sum(qty2) as INT8) " +
        "from table10 group by id");

    String expected = "id,?count_4,?avg_5,?min_6,?max_7,?sum_8,?cast_9,?cast_10,?cast_11,?cast_12\n" +
        "-------------------------------\n" +
        "1,2,4.0,3,5,12,4,3,5,12\n" +
        "2,3,2.0,1,3,6,7,6,8,21\n";

    assertEquals(expected, resultSetToString(res));

    // multiple distinct with expression
    res = executeString(
        "select count(distinct code) + count(distinct qty) from table10"
    );

    expected = "?plus_2\n" +
        "-------------------------------\n" +
        "9\n";

    assertEquals(expected, resultSetToString(res));
    res.close();

    res = executeString(
        "select id, count(distinct code) + count(distinct qty) from table10 group by id"
    );

    expected = "id,?plus_2\n" +
        "-------------------------------\n" +
        "1,5\n" +
        "2,6\n";

    assertEquals(expected, resultSetToString(res));
    res.close();

    executeString("DROP TABLE table10 PURGE").close();
  }

  @Test
  public final void testDistinctAggregationCaseByCase3() throws Exception {
    // first distinct is smaller than second distinct.
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("col1", Type.TEXT);
    schema.addColumn("col2", Type.TEXT);
    schema.addColumn("col3", Type.TEXT);

    String[] data = new String[]{
        "a|b-1|\\N",
        "a|b-2|\\N",
        "a|b-2|\\N",
        "a|b-3|\\N",
        "a|b-3|\\N",
        "a|b-3|\\N"
    };

    TajoTestingCluster.createTable("table10", schema, tableOptions, data);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE table10 PURGE").close();
  }

  @Test
  public final void testDistinctAggregationCaseByCase4() throws Exception {
    // Reproduction case for TAJO-994
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("col1", Type.TEXT);
    schema.addColumn("col2", Type.TEXT);

    String[] data = new String[]{
        "a|\\N",
        "a|\\N|",
        "a|\\N|",
        "a|\\N|",
        "a|\\N|",
        "a|\\N|"
    };

    TajoTestingCluster.createTable("testDistinctAggregationCaseByCase4".toLowerCase(), schema, tableOptions, data);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE testDistinctAggregationCaseByCase4 PURGE").close();
  }

  @Test
  public final void testComplexParameter() throws Exception {
    // select sum(l_extendedprice*l_discount) as revenue from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testComplexParameterWithSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testComplexParameter2() throws Exception {
    // select count(*) + max(l_orderkey) as merged from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testHavingWithNamedTarget() throws Exception {
    // select l_orderkey, avg(l_partkey) total, sum(l_linenumber) as num from lineitem group by l_orderkey
    // having total >= 2 or num = 3 order by l_orderkey, total;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testHavingWithAggFunction() throws Exception {
    // select l_orderkey, avg(l_partkey) total, sum(l_linenumber) as num from lineitem group by l_orderkey
    // having avg(l_partkey) = 2.5 or num = 1;
    runSimpleTests();
  }

  @Test
  public final void testGroupbyWithJson() throws Exception {
    // select l_orderkey, avg(l_partkey) total, sum(l_linenumber) as num from lineitem group by l_orderkey
    // having total >= 2 or num = 3;
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithNullData1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithNullData2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithNullData3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithNullData4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithNullData5() throws Exception {
    executeString("CREATE TABLE testGroupByWithNullData5 (age INT4, point FLOAT4);").close();
    assertTableExists("testGroupByWithNullData5".toLowerCase());

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE testGroupByWithNullData5");
  }

  @Test
  public final void testGroupByWithNullData6() throws Exception {
    executeString("CREATE TABLE testGroupByWithNullData6 (age INT4, point FLOAT4);").close();
    assertTableExists("testGroupByWithNullData6".toLowerCase());

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE testGroupByWithNullData6");
  }

  @Test
  public final void testGroupByWithNullData7() throws Exception {
    executeString("CREATE TABLE testGroupByWithNullData7 (age INT4, point FLOAT4);").close();
    assertTableExists("testGroupByWithNullData7".toLowerCase());

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE testGroupByWithNullData7");
  }

  @Test
  public final void testGroupByWithNullData8() throws Exception {
    executeString("CREATE TABLE testGroupByWithNullData8 (age INT4, point FLOAT4);").close();
    assertTableExists("testGroupByWithNullData8".toLowerCase());

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE testGroupByWithNullData8");
  }

  @Test
  public final void testGroupByWithNullData9() throws Exception {
    executeString("CREATE TABLE testGroupByWithNullData9 (age INT4, point FLOAT4);").close();
    assertTableExists("testGroupByWithNullData9".toLowerCase());

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE testGroupByWithNullData9");
  }

  @Test
  public final void testGroupByWithNullData10() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithNullData11() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByWithNullData12() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNumShufflePartition() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("col1", Type.TEXT);
    schema.addColumn("col2", Type.TEXT);

    List<String> data = new ArrayList<String>();
    int totalBytes = 0;
    Random rand = new Random(System.currentTimeMillis());
    String col1Prefix = "Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1" +
        "Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1" +
        "Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1Column-1";

    Set<Integer> uniqKeys = new HashSet<Integer>();
    while(true) {
      int col1RandomValue = rand.nextInt(1000000);
      uniqKeys.add(col1RandomValue);
      String str = (col1Prefix + "-" + col1RandomValue) + "|col2-" + rand.nextInt(1000000);
      data.add(str);

      totalBytes += str.getBytes().length;

      if (totalBytes > 3 * 1024 * 1024) {
        break;
      }
    }
    TajoTestingCluster.createTable("testnumshufflepartition", schema, tableOptions, data.toArray(new String[]{}), 3);

    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_GROUPBY_PARTITION_VOLUME.varname, "2");
      ResultSet res = executeString(
          "select col1 \n" +
              ",count(distinct col2) as cnt1\n" +
              "from testnumshufflepartition \n" +
              "group by col1"
      );

      int numRows = 0;
      while (res.next()) {
        numRows++;
      }
      assertEquals(uniqKeys.size(), numRows);

      QueryId queryId = getQueryId(res);
      QueryHistory queryHistory = testingCluster.getQueryHistory(queryId);

      int shuffles = 0;
      for (StageHistory stage : queryHistory.getStageHistories()) {
        if (stage.getExecutionBlockId().endsWith("_000001")) {
          // Getting the number of partitions. It should be 2.
          shuffles = stage.getNumShuffles();
        }
      }

      assertEquals(2, shuffles);
      executeString("DROP TABLE testnumshufflepartition PURGE").close();
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_GROUPBY_PARTITION_VOLUME.varname,
          ConfVars.$DIST_QUERY_GROUPBY_PARTITION_VOLUME.defaultVal);
    }
  }

  @Test
  public final void testGroupbyWithLimit1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupbyWithLimit2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupbyWithLimit3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testGroupbyWithPythonFunc() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testGroupbyWithPythonFunc2() throws Exception {
    runSimpleTests();
  }

  @Test
  public final void testPythonUdaf() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testPythonUdaf2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testPythonUdaf3() throws Exception {
    runSimpleTests();
  }

  // TODO: this test cannot be executed due to the bug of logical planner (TAJO-1588)
//  @Test
  public final void testPythonUdafWithHaving() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testPythonUdafWithNullData() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  // TODO: this test cannot be executed due to the bug of logical planner (TAJO-1588)
//  @Test
  public final void testComplexTargetWithPythonUdaf() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  // TODO: this test cannot be executed due to the bug of logical planner (TAJO-1588)
//  @Test
  public final void testDistinctPythonUdafWithUnion1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
