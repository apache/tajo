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
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.exec.DDLExecutor;
import org.apache.tajo.plan.*;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.KeyValueSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Stack;

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
  private final PlanShapeFixerContext planShapeFixerContext = new PlanShapeFixerContext();
  private final PlanShapeFixer verifyPreprocessor = new PlanShapeFixer();
  private final PidResetContext resetContext = new PidResetContext();
  private final PidReseter pidReseter = new PidReseter();

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

  public LogicalPlan buildLogicalPlan(String query) throws PlanningException {
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);
    optimizer.optimize(defaultContext, plan);
    planShapeFixerContext.reset();
    resetContext.reset();
    pidReseter.visit(resetContext, plan, plan.getRootBlock());
    verifyPreprocessor.visit(planShapeFixerContext, plan, plan.getRootBlock());

    return plan;
  }

  public MasterPlan buildMasterPlan(LogicalPlan plan) throws IOException, PlanningException {
    QueryId queryId = QueryIdFactory.newQueryId(0, 0);
    MasterPlan masterPlan = new MasterPlan(queryId, defaultContext, plan);
    globalPlanner.build(masterPlan);
    return masterPlan;
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

  private static class PidResetContext {
    int seqId = 0;
    public void reset() {
      seqId = 0;
    }
  }

  private static class PidReseter extends BasicLogicalPlanVisitor<PidResetContext, LogicalNode> {

    @Override
    public void preHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, PidResetContext context)
        throws PlanningException {
      node.setPID(context.seqId++);
    }
  }

  private static class PlanShapeFixerContext {

    Stack<Integer> childNumbers = new Stack<Integer>();
    public void reset() {
      childNumbers.clear();
    }
  }

  private static final ColumnComparator columnComparator = new ColumnComparator();
  private static final EvalNodeComparator evalNodeComparator = new EvalNodeComparator();
  private static final TargetComparator targetComparator = new TargetComparator();

  private static class PlanShapeFixer extends BasicLogicalPlanVisitor<PlanShapeFixerContext, LogicalNode> {

    @Override
    public LogicalNode visitScan(PlanShapeFixerContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 ScanNode node, Stack<LogicalNode> stack) throws PlanningException {
      super.visitScan(context, plan, block, node, stack);
      context.childNumbers.push(1);
      node.setInSchema(sortSchema(node.getInSchema()));
      if (node.hasQual()) {
        node.setQual(sortQual(node.getQual()));
      }
      return null;
    }

    @Override
    public LogicalNode visitJoin(PlanShapeFixerContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
      super.visitJoin(context, plan, block, node, stack);
      int rightChildNum = context.childNumbers.pop();
      int leftChildNum = context.childNumbers.pop();

      if (PlannerUtil.isCommutativeJoin(node.getJoinType())) {

        if (leftChildNum < rightChildNum) {
          swapChildren(node);
        } else if (leftChildNum == rightChildNum) {
          if (node.getLeftChild().toJson().compareTo(node.getRightChild().toJson()) <
              0) {
            swapChildren(node);
          }
        }
      }

      node.setInSchema(sortSchema(node.getInSchema()));
      node.setOutSchema(sortSchema(node.getOutSchema()));

      if (node.hasJoinQual()) {
        node.setJoinQual(sortQual(node.getJoinQual()));
      }

      if (node.hasTargets()) {
        node.setTargets(sortTargets(node.getTargets()));
      }

      context.childNumbers.push(rightChildNum + leftChildNum);

      return null;
    }

    private Schema sortSchema(Schema schema) {
      Column[] columns = schema.toArray();
      Arrays.sort(columns, columnComparator);

      Schema sorted = new Schema();
      for (Column col : columns) {
        sorted.addColumn(col);
      }
      return sorted;
    }

    private EvalNode sortQual(EvalNode qual) {
      EvalNode[] cnf = AlgebraicUtil.toConjunctiveNormalFormArray(qual);
      Arrays.sort(cnf, evalNodeComparator);
      return AlgebraicUtil.createSingletonExprFromCNF(cnf);
    }

    private Target[] sortTargets(Target[] targets) {
      Arrays.sort(targets, targetComparator);
      return targets;
    }

    private static void swapChildren(JoinNode node) {
      LogicalNode tmpChild = node.getLeftChild();
      int tmpId = tmpChild.getPID();
      tmpChild.setPID(node.getRightChild().getPID());
      node.getRightChild().setPID(tmpId);
      node.setLeftChild(node.getRightChild());
      node.setRightChild(tmpChild);
    }
  }

  private static class ColumnComparator implements Comparator<Column> {

    @Override
    public int compare(Column o1, Column o2) {
      return o1.getQualifiedName().compareTo(o2.getQualifiedName());
    }
  }

  private static class EvalNodeComparator implements Comparator<EvalNode> {

    @Override
    public int compare(EvalNode o1, EvalNode o2) {
      return o1.toJson().compareTo(o2.toJson());
    }
  }

  private static class TargetComparator implements Comparator<Target> {

    @Override
    public int compare(Target o1, Target o2) {
      return o1.toJson().compareTo(o2.toJson());
    }
  }
}
