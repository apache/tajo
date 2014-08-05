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

package org.apache.tajo.master.querymaster;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.worker.TajoWorker;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.util.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestQueryUnitStatusUpdate extends QueryTestCaseBase {

  public TestQueryUnitStatusUpdate() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    conf.set(TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.varname, "false");
  }

  @Test
  public final void case1() throws Exception {
    // select l_linenumber, count(1) as unique_key from lineitem group by l_linenumber;
    ResultSet res = null;
    try {
      res = executeQuery();

      // tpch/lineitem.tbl
      long[] expectedNumRows = new long[]{5, 2, 2, 2};
      long[] expectedNumBytes = new long[]{604, 18, 18, 8};
      long[] expectedReadBytes = new long[]{604, 18, 18, 0};

      assertStatus(2, expectedNumRows, expectedNumBytes, expectedReadBytes);
    } finally {
      cleanupQuery(res);
    }
  }

  @Test
  public final void case2() throws Exception {
    // ExternalMergeSort
    ResultSet res = null;
    try {
      res = executeQuery();

      // tpch/lineitem.tbl
      long[] expectedNumRows = new long[]{5, 2, 2, 2, 2, 2};
      long[] expectedNumBytes = new long[]{604, 162, 162, 138, 138, 194};
      long[] expectedReadBytes = new long[]{604, 162, 162, 0, 138, 0};

      assertStatus(3, expectedNumRows, expectedNumBytes, expectedReadBytes);
    } finally {
      cleanupQuery(res);
    }
  }


  @Test
  public final void case3() throws Exception {
    // Partition Scan
    ResultSet res = null;
    try {
      createColumnPartitionedTable();

      /*
      |-eb_1404143727281_0002_000005
         |-eb_1404143727281_0002_000004        (order by)
            |-eb_1404143727281_0002_000003     (join)
               |-eb_1404143727281_0002_000002  (scan)
               |-eb_1404143727281_0002_000001  (scan, filter)
       */
      res = executeQuery();

      String actualResult = resultSetToString(res);
      System.out.println(actualResult);

      // in/out * subquery(4)
      long[] expectedNumRows = new long[]{2, 2, 5, 5, 7, 2, 2, 2};
      long[] expectedNumBytes = new long[]{8, 34, 20, 75, 109, 34, 34, 18};
      long[] expectedReadBytes = new long[]{8, 34, 20, 75, 109, 0, 34, 0};

      assertStatus(4, expectedNumRows, expectedNumBytes, expectedReadBytes);
    } finally {
      cleanupQuery(res);
    }
  }

  private void createColumnPartitionedTable() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("ColumnPartitionedTable");
    ResultSet res = executeString(
        "create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8) ");
    res.close();

    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
    assertEquals(2, catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getSchema().size());
    assertEquals(3,
        catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName).getLogicalSchema().size());

    res = testBase.execute(
        "insert overwrite into " + tableName + " select l_orderkey, l_partkey, l_quantity from lineitem");

    res.close();
  }

  private void assertStatus(int numSubQueries,
                            long[] expectedNumRows,
                            long[] expectedNumBytes,
                            long[] expectedReadBytes) throws Exception {
      List<TajoWorker> tajoWorkers = testingCluster.getTajoWorkers();
      Collection<QueryMasterTask> finishedTasks = null;
      for (TajoWorker eachWorker: tajoWorkers) {
        finishedTasks = eachWorker.getWorkerContext().getQueryMaster().getFinishedQueryMasterTasks();
        if (finishedTasks != null && !finishedTasks.isEmpty()) {
          break;
        }
      }

      assertNotNull(finishedTasks);
      assertTrue(!finishedTasks.isEmpty());

      List<QueryMasterTask> finishedTaskList = new ArrayList<QueryMasterTask>(finishedTasks);

      Collections.sort(finishedTaskList, new Comparator<QueryMasterTask>() {
        @Override
        public int compare(QueryMasterTask o1, QueryMasterTask o2) {
          return o2.getQueryId().compareTo(o1.getQueryId());
        }
      });

      Query query = finishedTaskList.get(0).getQuery();

      assertNotNull(query);

      List<SubQuery> subQueries = new ArrayList<SubQuery>(query.getSubQueries());
      assertEquals(numSubQueries, subQueries.size());

      Collections.sort(subQueries, new Comparator<SubQuery>() {
        @Override
        public int compare(SubQuery o1, SubQuery o2) {
          return o1.getId().compareTo(o2.getId());
        }
      });

      int index = 0;
      for (SubQuery eachSubQuery: subQueries) {
        TableStats inputStats = eachSubQuery.getInputStats();
        TableStats resultStats = eachSubQuery.getResultStats();

        assertNotNull(inputStats);
        assertEquals(expectedNumRows[index], inputStats.getNumRows().longValue());
        assertEquals(expectedNumBytes[index], inputStats.getNumBytes().longValue());
        assertEquals(expectedReadBytes[index], inputStats.getReadBytes().longValue());

        index++;

        assertNotNull(resultStats);
        assertEquals(expectedNumRows[index], resultStats.getNumRows().longValue());
        assertEquals(expectedNumBytes[index], resultStats.getNumBytes().longValue());
        assertEquals(expectedReadBytes[index], resultStats.getReadBytes().longValue());

        index++;
      }

  }
}
