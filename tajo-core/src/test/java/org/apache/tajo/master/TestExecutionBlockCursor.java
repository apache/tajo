/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.master;

import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalOptimizer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.global.ExecutionBlockCursor;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;

public class TestExecutionBlockCursor {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static GlobalPlanner planner;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner logicalPlanner;
  private static LogicalOptimizer optimizer;
  private static AsyncDispatcher dispatcher;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();

    conf = util.getConfiguration();
    conf.set(TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.varname, "false");

    catalog = util.getMiniCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:!234/warehouse");
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    TPCH tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();
    for (String table : tpch.getTableNames()) {
      TableMeta m = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
      TableDesc d = CatalogUtil.newTableDesc(
          CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, table), tpch.getSchema(table), m, CommonTestingUtil.getTestDir());
      TableStats stats = new TableStats();
      stats.setNumBytes(TPCH.tableVolumes.get(table));
      d.setStats(stats);
      catalog.createTable(d);
    }

    analyzer = new SQLAnalyzer();
    logicalPlanner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);

    AbstractStorageManager sm  = StorageManagerFactory.getStorageManager(conf);
    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    planner = new GlobalPlanner(conf, catalog);
  }

  @AfterClass
  public static void tearDown() {
    util.shutdownCatalogCluster();
    if (dispatcher != null) {
      dispatcher.stop();
    }
  }

  @Test
  public void testNextBlock() throws Exception {
    Expr context = analyzer.parse(
        "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost, " +
            "r_name, p_type, p_size " +
            "from region join nation on n_regionkey = r_regionkey and r_name = 'AMERICA' " +
            "join supplier on s_nationkey = n_nationkey " +
            "join partsupp on s_suppkey = ps_suppkey " +
            "join part on p_partkey = ps_partkey and p_type like '%BRASS' and p_size = 15");
    LogicalPlan logicalPlan = logicalPlanner.createPlan(LocalTajoTestingUtility.createDummySession(), context);
    optimizer.optimize(logicalPlan);
    QueryContext queryContext = new QueryContext();
    MasterPlan plan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), queryContext, logicalPlan);
    planner.build(plan);

    ExecutionBlockCursor cursor = new ExecutionBlockCursor(plan);

    int count = 0;
    while(cursor.hasNext()) {
      cursor.nextBlock();
      count++;
    }

    /*
     |-eb_0000000000000_0001_000010
       |-eb_0000000000000_0001_000009
          |-eb_0000000000000_0001_000008
          |-eb_0000000000000_0001_000007
             |-eb_0000000000000_0001_000006
                |-eb_0000000000000_0001_000005
                |-eb_0000000000000_0001_000004
             |-eb_0000000000000_0001_000003
                |-eb_0000000000000_0001_000002
                |-eb_0000000000000_0001_000001
     */
    assertEquals(10, count);
  }
}
