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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.eval.BinaryEval;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.TUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestGlobalPlanner {
  private static Log LOG = LogFactory.getLog(TestGlobalPlanner.class);

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
    for (FunctionDesc funcDesc : FunctionLoader.findLegacyFunctions()) {
      catalog.createFunction(funcDesc);
    }
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);

    // TPC-H Schema for Complex Queries
    String [] tables = {
        "part", "supplier", "partsupp", "nation", "region", "lineitem", "orders", "customer", "customer_parts"
    };
    int [] volumes = {
        100, 200, 50, 5, 5, 800, 300, 100, 707
    };
    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();
    for (int i = 0; i < tables.length; i++) {
      TableMeta m = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
      TableStats stats = new TableStats();
      stats.setNumBytes(volumes[i]);
      TableDesc d = CatalogUtil.newTableDesc(
          CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, tables[i]), tpch.getSchema(tables[i]), m,
          CommonTestingUtil.getTestDir());
      d.setStats(stats);

      if (tables[i].equals(TPCH.CUSTOMER_PARTS)) {
        Schema expressionSchema = new Schema();
        expressionSchema.addColumn("c_nationkey", TajoDataTypes.Type.INT4);
        PartitionMethodDesc partitionMethodDesc = new PartitionMethodDesc(
            DEFAULT_DATABASE_NAME,
            tables[i],
            CatalogProtos.PartitionType.COLUMN,
            "c_nationkey",
            expressionSchema);

        d.setPartitionMethod(partitionMethodDesc);
      }
      catalog.createTable(d);
    }

    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(util.getConfiguration());
    globalPlanner = new GlobalPlanner(util.getConfiguration(), catalog);
  }

  @AfterClass
  public static void tearDown() {
    util.shutdownCatalogCluster();
  }

  private MasterPlan buildPlan(String sql) throws PlanningException, IOException {
    Expr expr = sqlAnalyzer.parse(sql);
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummyContext(util.getConfiguration()), expr);
    optimizer.optimize(plan);
    QueryContext context = new QueryContext(util.getConfiguration());
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
  public void testThetaJoinKeyPairs() throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("select n_nationkey, n_name, n_regionkey, t.cnt");
    sb.append(" from nation n");
    sb.append(" join");
    sb.append(" (");
    sb.append("   select r_regionkey, count(*) as cnt");
    sb.append("   from nation n");
    sb.append("   join region r on (n.n_regionkey = r.r_regionkey)");
    sb.append("   group by r_regionkey");
    sb.append(" ) t  on  (n.n_regionkey = t.r_regionkey)");
    sb.append(" and n.n_nationkey > t.cnt ");
    sb.append(" order by n_nationkey");

    MasterPlan plan = buildPlan(sb.toString());
    ExecutionBlock root = plan.getRoot();

    Map<BinaryEval, Boolean> evalMap = TUtil.newHashMap();
    BinaryEval eval1 = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_regionkey", TajoDataTypes.Type.INT4))
        , new FieldEval(new Column("default.t.r_regionkey", TajoDataTypes.Type.INT4))
    );
    evalMap.put(eval1, Boolean.FALSE);

    BinaryEval eval2 = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_nationkey", TajoDataTypes.Type.INT4))
        , new FieldEval(new Column("default.t.cnt", TajoDataTypes.Type.INT4))
    );
    evalMap.put(eval2, Boolean.FALSE);

    visitChildExecutionBLock(plan, root, evalMap);

    // Find required shuffleKey.
    assertTrue(evalMap.get(eval1).booleanValue());

    // Find that ShuffleKeys only includes equi-join conditions
    assertFalse(evalMap.get(eval2).booleanValue());
  }

  private void visitChildExecutionBLock(MasterPlan plan, ExecutionBlock parentBlock,
                                        Map<BinaryEval, Boolean> qualMap) throws Exception {
    boolean isExistLeftField, isExistRightField;

    for (Map.Entry<BinaryEval, Boolean> entry : qualMap.entrySet()) {
      FieldEval leftField = (FieldEval)entry.getKey().getLeftExpr();
      FieldEval rightField = (FieldEval)entry.getKey().getRightExpr();

      for (ExecutionBlock block : plan.getChilds(parentBlock))  {
        isExistLeftField = false;
        isExistRightField = false;

        if (plan.getIncomingChannels(block.getId()) != null) {
          for (DataChannel channel :plan.getIncomingChannels(block.getId())) {
            if (channel.getShuffleKeys() != null) {
              for (Column column : channel.getShuffleKeys()) {
                if (column.getQualifiedName().equals(leftField.getColumnRef().getQualifiedName())) {
                  isExistLeftField = true;
                } else if (column.getQualifiedName().
                    equals(rightField.getColumnRef().getQualifiedName())) {
                  isExistRightField = true;
                }
              }
            }
          }

          if(isExistLeftField && isExistRightField) {
            qualMap.put(entry.getKey(), Boolean.TRUE);
          }
        }

        visitChildExecutionBLock(plan, block, qualMap);
      }
    }
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

  @Test
  public void testCheckIfSimpleQuery() throws Exception {
    MasterPlan plan = buildPlan("select * from customer");
    assertTrue(PlannerUtil.checkIfSimpleQuery(plan.getLogicalPlan()));

    //partition table
    plan = buildPlan("select * from customer_parts");
    assertTrue(PlannerUtil.checkIfSimpleQuery(plan.getLogicalPlan()));

    plan = buildPlan("select * from customer where c_nationkey = 1");
    assertFalse(PlannerUtil.checkIfSimpleQuery(plan.getLogicalPlan()));

    plan = buildPlan("select * from customer_parts where c_nationkey = 1");
    assertFalse(PlannerUtil.checkIfSimpleQuery(plan.getLogicalPlan()));

    // same column order
    plan = buildPlan("select c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment" +
        " from customer");
    assertTrue(PlannerUtil.checkIfSimpleQuery(plan.getLogicalPlan()));

    plan = buildPlan("select c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_comment, c_nationkey " +
        " from customer_parts");
    assertTrue(PlannerUtil.checkIfSimpleQuery(plan.getLogicalPlan()));

    // different column order
    plan = buildPlan("select c_name, c_custkey, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment" +
        " from customer");
    assertFalse(PlannerUtil.checkIfSimpleQuery(plan.getLogicalPlan()));

    plan = buildPlan("select c_name, c_custkey, c_address, c_phone, c_acctbal, c_mktsegment, c_comment, c_nationkey " +
        " from customer_parts");
    assertFalse(PlannerUtil.checkIfSimpleQuery(plan.getLogicalPlan()));

    plan = buildPlan("insert into customer_parts " +
        " select c_name, c_custkey, c_address, c_phone, c_acctbal, c_mktsegment, c_comment, c_nationkey " +
        " from customer");
    assertFalse(PlannerUtil.checkIfSimpleQuery(plan.getLogicalPlan()));
  }
}
