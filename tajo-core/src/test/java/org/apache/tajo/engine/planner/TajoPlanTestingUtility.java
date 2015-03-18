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

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.exec.DDLExecutor;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.util.KeyValueSet;

import java.io.IOException;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;

public class TajoPlanTestingUtility {
  private static final Log LOG = LogFactory.getLog(TajoPlanTestingUtility.class);

  /**
   * Default parent directory for test output.
   */
  public static final String DEFAULT_TEST_DIRECTORY = "target/" +
      System.getProperty("tajo.test.data.dir", "test-data");

  private TajoConf conf;
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private QueryContext defaultContext;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private GlobalPlanner globalPlanner;
  private DDLExecutor ddlExecutor;

  public void setup(String[] names,
                    String[] tablepaths,
                    Schema[] schemas,
                    KeyValueSet option) throws Exception {
    util = new TajoTestingCluster();
    conf = util.getConfiguration();
    conf.setLongVar(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD, 500 * 1024);
    conf.setBoolVar(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED, true);

    catalog = util.startCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, conf.getVar(TajoConf.ConfVars.WAREHOUSE_DIR));
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    util.getMiniCatalogCluster().getCatalogServer().reloadBuiltinFunctions(FunctionLoader.findLegacyFunctions());

    defaultContext = LocalTajoTestingUtility.createDummyContext(conf);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);
    globalPlanner = new GlobalPlanner(conf, catalog);
    ddlExecutor = new DDLExecutor(catalog);

//    FileSystem fs = FileSystem.getLocal(conf);
//    Path rootDir = TajoConf.getWarehouseDir(conf);
//    fs.mkdirs(rootDir);
//    for (int i = 0; i < tablepaths.length; i++) {
//      Path localPath = new Path(tablepaths[i]);
//      Path tablePath = new Path(rootDir, names[i]);
//      fs.mkdirs(tablePath);
//      Path dfsPath = new Path(tablePath, localPath.getName());
//      fs.copyFromLocalFile(localPath, dfsPath);
//      TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV, option);
//
//      // Add fake table statistic data to tables.
//      // It gives more various situations to unit tests.
//      TableStats stats = new TableStats();
//      stats.setNumBytes(TPCH.tableVolumes.get(names[i]));
//      TableDesc tableDesc = new TableDesc(
//          CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, names[i]), schemas[i], meta,
//          tablePath.toUri());
//      tableDesc.setStats(stats);
//      catalog.createTable(tableDesc);
//    }

    for (int i = 0; i < tablepaths.length; i++) {
      ddlExecutor.createTable(defaultContext, CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, names[i]),
          CatalogProtos.StoreType.CSV, schemas[i], CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV, option),
          new Path(tablepaths[i]), true, null, true);
    }
  }

  public void shutdown() {
    util.shutdownCatalogCluster();
  }

  public String execute(String query) throws PlanningException, IOException {
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);
    optimizer.optimize(defaultContext, plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    MasterPlan masterPlan = new MasterPlan(queryId, defaultContext, plan);
    globalPlanner.build(masterPlan);

    StringBuilder sb = new StringBuilder("********** Logical plan **********").append("\n\n");
    sb.append(masterPlan.getLogicalPlan().getLogicalPlanAsString()).append("\n\n");
    sb.append("********** Master plan **********").append("\n\n").append(masterPlan.toString());

    return sb.toString();
  }

  public TajoConf getConf() {
    return conf;
  }

  public CatalogService getCatalog() {
    return catalog;
  }

  public SQLAnalyzer getSQLAnalyzer() {
    return analyzer;
  }

  public boolean executeDDL(String query) throws PlanningException, IOException {
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);
    optimizer.optimize(defaultContext, plan);

    return ddlExecutor.execute(defaultContext, plan);
  }
}
