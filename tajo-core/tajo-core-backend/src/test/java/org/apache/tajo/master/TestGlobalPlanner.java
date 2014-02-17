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

package org.apache.tajo.master;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalOptimizer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestGlobalPlanner {

  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer sqlAnalyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static TPCH tpch;
  private static GlobalPlanner globalPlanner;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }

    // TPC-H Schema for Complex Queries
    String [] tables = {
        "part", "supplier", "partsupp", "nation", "region", "lineitem", "orders", "customer"
    };
    int [] volumes = {
        100, 200, 50, 5, 5, 800, 300, 100
    };
    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();
    for (int i = 0; i < tables.length; i++) {
      TableMeta m = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
      TableStats stats = new TableStats();
      stats.setNumBytes(volumes[i]);
      TableDesc d = CatalogUtil.newTableDesc(tables[i], tpch.getSchema(tables[i]), m, CommonTestingUtil.getTestDir());
      d.setStats(stats);
      catalog.addTable(d);
    }

    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(util.getConfiguration());
    globalPlanner = new GlobalPlanner(util.getConfiguration(),
        StorageManagerFactory.getStorageManager(util.getConfiguration()));
  }

  @AfterClass
  public static void tearDown() {
    util.shutdownCatalogCluster();
  }

  private MasterPlan buildPlan(String sql) throws PlanningException, IOException {
    Expr expr = sqlAnalyzer.parse(sql);
    LogicalPlan plan = planner.createPlan(expr);
    optimizer.optimize(plan);
    QueryContext context = new QueryContext();
    MasterPlan masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), context, plan);
    globalPlanner.build(masterPlan);
    return masterPlan;
  }

  @Test
  public void testSelectDistinct() throws Exception {
    buildPlan("select distinct l_orderkey from lineitem");
  }

  @Test
  public void testSortAfterGroupBy() throws Exception {
    buildPlan("select max(l_quantity) as max_quantity, l_orderkey from lineitem group by l_orderkey order by max_quantity");
  }

  @Test
  public void testSortLimit() throws Exception {
    buildPlan("select max(l_quantity) as max_quantity, l_orderkey from lineitem group by l_orderkey order by max_quantity limit 3");
  }

  @Test
  public void testJoin() throws Exception {
    buildPlan("select n_name, r_name, n_regionkey, r_regionkey from nation, region");
  }

  @Test
  public void testUnion() throws IOException, PlanningException {
    buildPlan("select o_custkey as num from orders union select c_custkey as num from customer union select p_partkey as num from part");
  }

  @Test
  public void testSubQuery() throws IOException, PlanningException {
    buildPlan("select l.l_orderkey from (select * from lineitem) l");
  }

  @Test
  public void testSubQueryJoin() throws IOException, PlanningException {
    buildPlan("select l.l_orderkey from (select * from lineitem) l join (select * from orders) o on l.l_orderkey = o.o_orderkey");
  }

  @Test
  public void testSubQueryGroupBy() throws IOException, PlanningException {
    buildPlan("select sum(l_extendedprice*l_discount) as revenue from (select * from lineitem) as l");
  }

  @Test
  public void testSubQueryGroupBy2() throws IOException, PlanningException {
    buildPlan("select l_orderkey, sum(l_extendedprice*l_discount)  as revenue from (select * from lineitem) as l group by l_orderkey");
  }

  @Test
  public void testSubQuerySortAfterGroup() throws IOException, PlanningException {
    buildPlan("select l_orderkey, sum(l_extendedprice*l_discount)  as revenue from (select * from lineitem) as l group by l_orderkey order by l_orderkey");
  }

  @Test
  public void testSubQuerySortAfterGroupMultiBlocks() throws IOException, PlanningException {
    buildPlan(
        "select l_orderkey, revenue from (" +
          "select l_orderkey, sum(l_extendedprice*l_discount) as revenue from lineitem group by l_orderkey"
        +") l1"

    );
  }

  @Test
  public void testSubQuerySortAfterGroupMultiBlocks2() throws IOException, PlanningException {
    buildPlan(
        "select l_orderkey, revenue from (" +
          "select l_orderkey, revenue from (" +
              "select l_orderkey, sum(l_extendedprice*l_discount) as revenue from lineitem group by l_orderkey"
              +") l1" +
          ") l2 order by l_orderkey"

    );
  }

  @Test
  public void testComplexUnion1() throws Exception {
    buildPlan(FileUtil.readTextFile(new File("src/test/resources/queries/default/complex_union_1.sql")));
  }

  @Test
  public void testComplexUnion2() throws Exception {
    buildPlan(FileUtil.readTextFile(new File("src/test/resources/queries/default/complex_union_2.sql")));
  }

  @Test
  public void testUnionGroupBy1() throws Exception {
    buildPlan("select l_orderkey, sum(l_extendedprice*l_discount) as revenue from (" +
        "select * from lineitem " +
        "union " +
        "select * from lineitem ) l group by l_orderkey");
  }

  @Test
  public void testTPCH_Q5() throws Exception {
    buildPlan(FileUtil.readTextFile(new File("benchmark/tpch/q5.sql")));
  }
}
