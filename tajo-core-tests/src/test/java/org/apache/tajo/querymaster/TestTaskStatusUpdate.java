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

package org.apache.tajo.querymaster;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.history.QueryHistory;
import org.apache.tajo.util.history.StageHistory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestTaskStatusUpdate extends QueryTestCaseBase {

  public TestTaskStatusUpdate() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    conf.set(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "false");
  }

  @Test
  public final void case1() throws Exception {
    // select l_linenumber, count(1) as unique_key from lineitem group by l_linenumber;
    ResultSet res = null;
    try {
      res = executeQuery();

      // tpch/lineitem.tbl
      long[] expectedNumRows = new long[]{5, 2, 2, 2};
      long[] expectedNumBytes = new long[]{604, 18, 18, 48};
      long[] expectedReadBytes = new long[]{604, 604, 18, 0};
      QueryId queryId = getQueryId(res);

      assertStatus(queryId, 2, expectedNumRows, expectedNumBytes, expectedReadBytes);
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
      long[] expectedNumBytes = new long[]{604, 162, 162, 138, 138, 236};
      long[] expectedReadBytes = new long[]{604, 604, 162, 0, 138, 0};

      QueryId queryId = getQueryId(res);
      assertStatus(queryId, 3, expectedNumRows, expectedNumBytes, expectedReadBytes);
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
               |-eb_1404143727281_0002_000002  (scan, filter)
               |-eb_1404143727281_0002_000001  (scan)
       */
      res = executeQuery();

      // in/out * stage(4)
      long[] expectedNumRows = new long[]{5, 5, 2, 2, 7, 2, 2, 2};
      long[] expectedNumBytes = new long[]{20, 75, 8, 34, 109, 34, 34, 64};
      long[] expectedReadBytes = new long[]{20, 20, 8, 8, 109, 0, 34, 0};

      QueryId queryId = getQueryId(res);
      assertStatus(queryId, 4, expectedNumRows, expectedNumBytes, expectedReadBytes);
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

  private void assertStatus(QueryId queryId, int numStages,
                            long[] expectedNumRows,
                            long[] expectedNumBytes,
                            long[] expectedReadBytes) throws Exception {


      QueryHistory queryHistory  = testingCluster.getQueryHistory(queryId);

      assertNotNull(queryHistory);

      List<StageHistory> stages = queryHistory.getStageHistories();
      assertEquals(numStages, stages.size());

      Collections.sort(stages, new Comparator<StageHistory>() {
        @Override
        public int compare(StageHistory o1, StageHistory o2) {
          return o1.getExecutionBlockId().compareTo(o2.getExecutionBlockId());
        }
      });

      int index = 0;
      for (StageHistory eachStage : stages) {

        assertEquals(expectedNumRows[index], eachStage.getTotalReadRows());
        assertEquals(expectedNumBytes[index], eachStage.getTotalInputBytes());
        assertEquals(expectedReadBytes[index], eachStage.getTotalReadBytes());

        index++;

        assertEquals(expectedNumRows[index], eachStage.getTotalWriteRows());
        assertEquals(expectedNumBytes[index],eachStage.getTotalWriteBytes());

        index++;
      }

  }
}
