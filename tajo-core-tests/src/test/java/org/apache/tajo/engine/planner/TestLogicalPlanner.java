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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tajo.*;
import org.apache.tajo.algebra.AlterTableOpType;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.function.builtin.SumInt;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.session.Session;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.*;

public class TestLogicalPlanner {
  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer sqlAnalyzer;
  private static LogicalPlanner planner;
  private static TPCH tpch;
  private static Session session = LocalTajoTestingUtility.createDummySession();

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getCatalogService();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234");
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);

    for (FunctionDesc funcDesc : FunctionLoader.findLegacyFunctions()) {
      catalog.createFunction(funcDesc);
    }

    Schema schema = SchemaBuilder.builder()
        .add("name", Type.TEXT)
        .add("empid", Type.INT4)
        .add("deptname", Type.TEXT)
        .build();

    Schema schema2 = SchemaBuilder.builder()
        .add("deptname", Type.TEXT)
        .add("manager", Type.TEXT)
        .build();

    Schema schema3 = SchemaBuilder.builder()
        .add("deptname", Type.TEXT)
        .add("score", Type.INT4)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration());
    TableDesc people = new TableDesc(
        IdentifierUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, meta,
        CommonTestingUtil.getTestDir().toUri());
    catalog.createTable(people);

    TableDesc student = new TableDesc(
        IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), schema2, "TEXT", new KeyValueSet(),
        CommonTestingUtil.getTestDir().toUri());
    catalog.createTable(student);

    TableDesc score = new TableDesc(
        IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), schema3, "TEXT", new KeyValueSet(),
        CommonTestingUtil.getTestDir().toUri());
    catalog.createTable(score);

    FunctionDesc funcDesc = new FunctionDesc("sumtest", SumInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4));


    // TPC-H Schema for Complex Queries
    String [] tpchTables = {
        "part", "supplier", "partsupp", "nation", "region", "lineitem"
    };
    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();
    for (String table : tpchTables) {
      TableMeta m = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration());
      TableDesc d = CatalogUtil.newTableDesc(
          IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, table), tpch.getSchema(table), m,
          CommonTestingUtil.getTestDir());
      catalog.createTable(d);
    }

    catalog.createFunction(funcDesc);
    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  static String[] QUERIES = {
      "select name, empid, deptname from employee where empId > 500", // 0
      "select name, empid, e.deptname, manager from employee as e, dept as dp", // 1
      "select name, empid, e.deptname, manager, score from employee as e, dept, score", // 2
      "select p.deptname, sumtest(score) from dept as p, score group by p.deptName having sumtest(score) > 30", // 3
      "select p.deptname, score*200 from dept as p, score order by score*10 asc", // 4
      "select name from employee where empId = 100", // 5
      "select name, score from employee, score", // 6
      "select p.deptName, sumtest(score) from dept as p, score group by p.deptName", // 7
      "create table store1 as select p.deptName, sumtest(score) from dept as p, score group by p.deptName", // 8
      "select deptName, sumtest(score) from score group by deptName having sumtest(score) > 30", // 9
      "select 7 + 8 as res1, 8 * 9 as res2, 10 * 10 as res3", // 10
      "create index idx_employee on employee using bitmap_idx (name nulls first, empId desc) where empid > 100", // 11
      "select name, score from employee, score order by score limit 3", // 12
      "select length(name), length(deptname), *, empid+10 from employee where empId > 500", // 13
  };

  private static QueryContext createQueryContext() {
    QueryContext qc = new QueryContext(util.getConfiguration(), session);
    qc.put(QueryVars.DEFAULT_SPACE_URI, "file:/");
    qc.put(QueryVars.DEFAULT_SPACE_ROOT_URI, "file:/");
    return qc;
  }

  public static final void testCloneLogicalNode(LogicalNode n1) throws CloneNotSupportedException {
    LogicalNode copy = (LogicalNode) n1.clone();
    assertTrue(n1.deepEquals(copy));
  }

  @Test
  public final void testSingleRelation() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[0]);
    LogicalPlan planNode = planner.createPlan(qc, expr);
    LogicalNode plan = planNode.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    testCloneLogicalNode(plan);
    LogicalRootNode root = (LogicalRootNode) plan;

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = projNode.getChild();

    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), scanNode.getTableName());
  }

  public static void assertSchema(Schema expected, Schema schema) {
    Column expectedColumn;
    Column column;
    for (int i = 0; i < expected.size(); i++) {
      expectedColumn = expected.getColumn(i);
      column = schema.getColumn(expectedColumn.getSimpleName());
      assertEquals(expectedColumn.getSimpleName(), column.getSimpleName());
      assertEquals(expectedColumn.getDataType(), column.getDataType());
    }
  }

  @Test
  public final void testImplicityJoinPlan() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[1]);
    LogicalPlan planNode = planner.createPlan(qc, expr);
    LogicalNode plan = planNode.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    testCloneLogicalNode(root);

    Schema expectedSchema = SchemaBuilder.builder()
        .add("name", Type.TEXT)
        .add("empid", Type.INT4)
        .add("deptname", Type.TEXT)
        .add("manager", Type.TEXT)
        .build();
    for (int i = 0; i < expectedSchema.size(); i++) {
      Column found = root.getOutSchema().getColumn(expectedSchema.getColumn(i).getSimpleName());
      assertEquals(expectedSchema.getColumn(i).getDataType(), found.getDataType());
    }

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    JoinNode joinNode = projNode.getChild();

    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType());
    ScanNode leftNode = joinNode.getLeftChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), leftNode.getTableName());
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
    ScanNode rightNode = joinNode.getRightChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), rightNode.getTableName());

    // three relations
    expr = sqlAnalyzer.parse(QUERIES[2]);
    plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    testCloneLogicalNode(plan);

    expectedSchema = SchemaBuilder.builder().addAll(expectedSchema.getRootColumns())
        .add("score", Type.INT4)
        .build();
    assertSchema(expectedSchema, plan.getOutSchema());

    assertEquals(NodeType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    projNode = root.getChild();

    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    joinNode = projNode.getChild();

    assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());

    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
    ScanNode scan1 = joinNode.getRightChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), scan1.getTableName());

    JoinNode leftNode2 = joinNode.getLeftChild();
    assertEquals(NodeType.JOIN, leftNode2.getType());

    assertEquals(NodeType.SCAN, leftNode2.getLeftChild().getType());
    ScanNode leftScan = leftNode2.getLeftChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), leftScan.getTableName());

    assertEquals(NodeType.SCAN, leftNode2.getRightChild().getType());
    ScanNode rightScan = leftNode2.getRightChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), rightScan.getTableName());
  }



  String [] JOINS = {
      "select name, dept.deptName, score from employee natural join dept natural join score", // 0
      "select name, dept.deptName, score from employee inner join dept on employee.deptName = dept.deptName inner join score on dept.deptName = score.deptName", // 1
      "select name, dept.deptName, score from employee left outer join dept on employee.deptName = dept.deptName right outer join score on dept.deptName = score.deptName" // 2
  };

  static Schema expectedJoinSchema;
  static {
    expectedJoinSchema = SchemaBuilder.builder()
        .add("name", Type.TEXT)
        .add("deptname", Type.TEXT)
        .add("score", Type.INT4)
        .build();
  }

  @Test
  public final void testNaturalJoinPlan() throws TajoException {
    QueryContext qc = createQueryContext();
    // two relations
    Expr context = sqlAnalyzer.parse(JOINS[0]);
    LogicalNode plan = planner.createPlan(qc, context).getRootBlock().getRoot();
    assertSchema(expectedJoinSchema, plan.getOutSchema());

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode proj = root.getChild();
    assertEquals(NodeType.JOIN, proj.getChild().getType());
    JoinNode join = proj.getChild();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(NodeType.SCAN, join.getRightChild().getType());
    assertTrue(join.hasJoinQual());
    ScanNode scan = join.getRightChild();
    assertEquals("default.score", scan.getTableName());

    assertEquals(NodeType.JOIN, join.getLeftChild().getType());
    join = join.getLeftChild();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(NodeType.SCAN, join.getLeftChild().getType());
    ScanNode outer = join.getLeftChild();
    assertEquals("default.employee", outer.getTableName());
    assertEquals(NodeType.SCAN, join.getRightChild().getType());
    ScanNode inner = join.getRightChild();
    assertEquals("default.dept", inner.getTableName());
  }

  @Test
  public final void testInnerJoinPlan() throws TajoException {
    QueryContext qc = createQueryContext();
    // two relations
    Expr expr = sqlAnalyzer.parse(JOINS[1]);
    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    assertSchema(expectedJoinSchema, root.getOutSchema());

    assertEquals(NodeType.ROOT, root.getType());
    assertEquals(NodeType.PROJECTION, ((LogicalRootNode)root).getChild().getType());
    ProjectionNode proj = ((LogicalRootNode)root).getChild();
    assertEquals(NodeType.JOIN, proj.getChild().getType());
    JoinNode join = proj.getChild();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(NodeType.SCAN, join.getRightChild().getType());
    ScanNode scan = join.getRightChild();
    assertEquals("default.score", scan.getTableName());

    assertEquals(NodeType.JOIN, join.getLeftChild().getType());
    join = join.getLeftChild();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(NodeType.SCAN, join.getLeftChild().getType());
    ScanNode outer = join.getLeftChild();
    assertEquals("default.employee", outer.getTableName());
    assertEquals(NodeType.SCAN, join.getRightChild().getType());
    ScanNode inner = join.getRightChild();
    assertEquals("default.dept", inner.getTableName());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalType.EQUAL, join.getJoinQual().getType());
  }

  @Test
  public final void testOuterJoinPlan() throws TajoException {
    QueryContext qc = createQueryContext();

    // two relations
    Expr expr = sqlAnalyzer.parse(JOINS[2]);
    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    assertSchema(expectedJoinSchema, plan.getOutSchema());

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode proj = root.getChild();
    assertEquals(NodeType.JOIN, proj.getChild().getType());
    JoinNode join = proj.getChild();
    assertEquals(JoinType.RIGHT_OUTER, join.getJoinType());
    assertEquals(NodeType.SCAN, join.getRightChild().getType());
    ScanNode scan = join.getRightChild();
    assertEquals("default.score", scan.getTableName());

    assertEquals(NodeType.JOIN, join.getLeftChild().getType());
    join = join.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, join.getJoinType());
    assertEquals(NodeType.SCAN, join.getLeftChild().getType());
    ScanNode outer = join.getLeftChild();
    assertEquals("default.employee", outer.getTableName());
    assertEquals(NodeType.SCAN, join.getRightChild().getType());
    ScanNode inner = join.getRightChild();
    assertEquals("default.dept", inner.getTableName());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalType.EQUAL, join.getJoinQual().getType());
  }


  @Test
  public final void testGroupby() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    // without 'having clause'
    Expr context = sqlAnalyzer.parse(QUERIES[7]);
    LogicalNode plan = planner.createPlan(qc, context).getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    testQuery7(root.getChild());

    // with having clause
    context = sqlAnalyzer.parse(QUERIES[3]);
    plan = planner.createPlan(qc, context).getRootBlock().getRoot();
    testCloneLogicalNode(plan);

    assertEquals(NodeType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();
    assertEquals(NodeType.HAVING, projNode.getChild().getType());
    HavingNode havingNode = projNode.getChild();
    assertEquals(NodeType.GROUP_BY, havingNode.getChild().getType());
    GroupbyNode groupByNode =  havingNode.getChild();

    assertEquals(NodeType.JOIN, groupByNode.getChild().getType());
    JoinNode joinNode = groupByNode.getChild();

    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType());
    ScanNode leftNode = joinNode.getLeftChild();
    assertEquals("default.dept", leftNode.getTableName());
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
    ScanNode rightNode = joinNode.getRightChild();
    assertEquals("default.score", rightNode.getTableName());

    //LogicalOptimizer.optimize(context, plan);
  }


  @Test
  public final void testMultipleJoin() throws IOException, TajoException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File("src/test/resources/queries/TestJoinQuery/testTPCHQ2Join.sql")));
    QueryContext qc = createQueryContext();
    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    Schema expected = tpch.getOutSchema("q2");
    assertSchema(expected, plan.getOutSchema());
  }

  private final void findJoinQual(EvalNode evalNode, Map<BinaryEval, Boolean> qualMap,
                                  EvalType leftType, EvalType rightType)
      throws IOException, TajoException {
    Preconditions.checkArgument(evalNode instanceof BinaryEval);
    BinaryEval qual = (BinaryEval)evalNode;

    if (qual.getLeftExpr().getType() == leftType && qual.getRightExpr().getType() == rightType) {
      assertEquals(qual.getLeftExpr().getType(), EvalType.FIELD);
      FieldEval leftField = (FieldEval)qual.getLeftExpr();

      for (Map.Entry<BinaryEval, Boolean> entry : qualMap.entrySet()) {
        FieldEval leftJoinField = (FieldEval)entry.getKey().getLeftExpr();

        if (qual.getRightExpr().getType() == entry.getKey().getRightExpr().getType()) {
          if (rightType == EvalType.FIELD) {
            FieldEval rightField = (FieldEval)qual.getRightExpr();
            FieldEval rightJoinField = (FieldEval)entry.getKey().getRightExpr();

            if (leftJoinField.getColumnRef().getQualifiedName().equals(leftField.getColumnRef().getQualifiedName())
                && rightField.getColumnRef().getQualifiedName().equals(rightJoinField.getColumnRef().getQualifiedName())) {
              qualMap.put(entry.getKey(), Boolean.TRUE);
            }
          } else if (rightType == EvalType.CONST) {
            ConstEval rightField = (ConstEval)qual.getRightExpr();
            ConstEval rightJoinField = (ConstEval)entry.getKey().getRightExpr();

            if (leftJoinField.getColumnRef().getQualifiedName().equals(leftField.getColumnRef().getQualifiedName()) &&
                rightField.getValue().equals(rightJoinField.getValue())) {
              qualMap.put(entry.getKey(), Boolean.TRUE);
            }
          } else if (rightType == EvalType.ROW_CONSTANT) {
            RowConstantEval rightField = qual.getRightExpr();
            RowConstantEval rightJoinField = entry.getKey().getRightExpr();

            if (leftJoinField.getColumnRef().getQualifiedName().equals(leftField.getColumnRef().getQualifiedName())) {
              assertEquals(rightField.getValues().length, rightJoinField.getValues().length);
              for (int i = 0; i < rightField.getValues().length; i++) {
                assertEquals(rightField.getValues()[i], rightJoinField.getValues()[i]);
              }
              qualMap.put(entry.getKey(), Boolean.TRUE);
            }
          }
        }
      }
    }
  }

  @Test
  public final void testJoinWithMultipleJoinQual1() throws IOException, TajoException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File
            ("src/test/resources/queries/TestJoinQuery/testJoinWithMultipleJoinQual1.sql")));
    QueryContext qc = createQueryContext();

    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalNode node = plan.getRootBlock().getRoot();

    Schema expected = tpch.getOutSchema("q2");
    assertSchema(expected, node.getOutSchema());

    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    LogicalNode[] nodes = PlannerUtil.findAllNodes(node, NodeType.JOIN);
    Map<BinaryEval, Boolean> qualMap = new HashMap<>();
    BinaryEval joinQual = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_regionkey", Type.INT4))
        , new FieldEval(new Column("default.ps.ps_suppkey", Type.INT4))
        );
    qualMap.put(joinQual, Boolean.FALSE);

    for(LogicalNode eachNode : nodes) {
      JoinNode joinNode = (JoinNode)eachNode;
      EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual());

      for(EvalNode evalNode : evalNodes) {
        findJoinQual(evalNode, qualMap, EvalType.FIELD, EvalType.FIELD);
      }
    }

    for (Map.Entry<BinaryEval, Boolean> entry : qualMap.entrySet()) {
      if (!entry.getValue()) {
        Preconditions.checkArgument(false,
            "JoinQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }
  }

  @Test
  public final void testJoinWithMultipleJoinQual2() throws IOException, TajoException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File
            ("src/test/resources/queries/TestJoinQuery/testJoinWithMultipleJoinQual2.sql")));
    QueryContext qc = createQueryContext();

    LogicalPlan plan = planner.createPlan(qc,expr);
    LogicalNode node = plan.getRootBlock().getRoot();

    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    LogicalNode[] nodes = PlannerUtil.findAllNodes(node, NodeType.SCAN);
    Map<BinaryEval, Boolean> qualMap = new HashMap<>();
    BinaryEval joinQual = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_name", Type.TEXT))
        , new ConstEval(new TextDatum("MOROCCO"))
    );
    qualMap.put(joinQual, Boolean.FALSE);

    for(LogicalNode eachNode : nodes) {
      ScanNode scanNode = (ScanNode)eachNode;
      if (scanNode.hasQual()) {
        EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(scanNode.getQual());

        for(EvalNode evalNode : evalNodes) {
          findJoinQual(evalNode, qualMap, EvalType.FIELD, EvalType.CONST);
        }
      }
    }

    for (Map.Entry<BinaryEval, Boolean> entry : qualMap.entrySet()) {
      if (!entry.getValue()) {
        Preconditions.checkArgument(false,
            "SelectionQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }
  }

  @Test
  public final void testJoinWithMultipleJoinQual3() throws IOException, TajoException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File
            ("src/test/resources/queries/TestJoinQuery/testJoinWithMultipleJoinQual3.sql")));
    QueryContext qc = createQueryContext();

    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalNode node = plan.getRootBlock().getRoot();

    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    LogicalNode[] nodes = PlannerUtil.findAllNodes(node, NodeType.SCAN);
    Map<BinaryEval, Boolean> qualMap = new HashMap<>();
    TextDatum[] datums = new TextDatum[3];
    datums[0] = new TextDatum("ARGENTINA");
    datums[1] = new TextDatum("ETHIOPIA");
    datums[2] = new TextDatum("MOROCCO");

    BinaryEval joinQual = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_name", Type.TEXT))
        , new RowConstantEval(datums)
    );
    qualMap.put(joinQual, Boolean.FALSE);

    for(LogicalNode eachNode : nodes) {
      ScanNode scanNode = (ScanNode)eachNode;
      if (scanNode.hasQual()) {
        EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(scanNode.getQual());

        for(EvalNode evalNode : evalNodes) {
          findJoinQual(evalNode, qualMap, EvalType.FIELD, EvalType.ROW_CONSTANT);
        }
      }
    }

    for (Map.Entry<BinaryEval, Boolean> entry : qualMap.entrySet()) {
      if (!entry.getValue()) {
        Preconditions.checkArgument(false,
            "ScanQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }
  }


  @Test
  public final void testJoinWithMultipleJoinQual4() throws IOException, TajoException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File
            ("src/test/resources/queries/TestJoinQuery/testJoinWithMultipleJoinQual4.sql")));
    QueryContext qc = createQueryContext();

    LogicalPlan plan = planner.createPlan(qc, expr);
    LogicalNode node = plan.getRootBlock().getRoot();

    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration(), catalog, TablespaceManager.getInstance());
    optimizer.optimize(plan);

    Map<BinaryEval, Boolean> scanMap = new HashMap<>();
    TextDatum[] datums = new TextDatum[3];
    datums[0] = new TextDatum("ARGENTINA");
    datums[1] = new TextDatum("ETHIOPIA");
    datums[2] = new TextDatum("MOROCCO");

    BinaryEval scanQual = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_name", Type.TEXT))
        , new RowConstantEval(datums)
    );
    scanMap.put(scanQual, Boolean.FALSE);

    Map<BinaryEval, Boolean> joinQualMap = new HashMap<>();
    BinaryEval joinQual = new BinaryEval(EvalType.GTH
        , new FieldEval(new Column("default.t.n_nationkey", Type.INT4))
        , new FieldEval(new Column("default.s.s_suppkey", Type.INT4))
    );

    /* following code is commented because theta join is not supported yet
     * TODO It SHOULD be restored after TAJO-742 is resolved. */
    //joinQualMap.put(joinQual, Boolean.FALSE);

    LogicalNode[] nodes = PlannerUtil.findAllNodes(node, NodeType.JOIN);
    for(LogicalNode eachNode : nodes) {
      JoinNode joinNode = (JoinNode)eachNode;
      if (joinNode.hasJoinQual()) {
        EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual());

        for(EvalNode evalNode : evalNodes) {
          findJoinQual(evalNode, joinQualMap, EvalType.FIELD, EvalType.FIELD);
        }
      }
    }

    nodes = PlannerUtil.findAllNodes(node, NodeType.SCAN);
    for(LogicalNode eachNode : nodes) {
      ScanNode scanNode = (ScanNode)eachNode;
      if (scanNode.hasQual()) {
        EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(scanNode.getQual());

        for(EvalNode evalNode : evalNodes) {
          findJoinQual(evalNode, scanMap, EvalType.FIELD, EvalType.ROW_CONSTANT);
        }
      }
    }


    for (Map.Entry<BinaryEval, Boolean> entry : joinQualMap.entrySet()) {
      if (!entry.getValue()) {
        Preconditions.checkArgument(false,
            "JoinQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }

    for (Map.Entry<BinaryEval, Boolean> entry : scanMap.entrySet()) {
      if (!entry.getValue()) {
        Preconditions.checkArgument(false,
            "ScanQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }
  }

  static void testQuery7(LogicalNode plan) {
    assertEquals(NodeType.PROJECTION, plan.getType());
    ProjectionNode projNode = (ProjectionNode) plan;
    assertEquals(NodeType.GROUP_BY, projNode.getChild().getType());
    GroupbyNode groupByNode = projNode.getChild();

    assertEquals(NodeType.JOIN, groupByNode.getChild().getType());
    JoinNode joinNode = groupByNode.getChild();

    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType());
    ScanNode leftNode = joinNode.getLeftChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), leftNode.getTableName());
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
    ScanNode rightNode = joinNode.getRightChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), rightNode.getTableName());
  }


  @Test
  public final void testStoreTable() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr context = sqlAnalyzer.parse(QUERIES[8]);

    LogicalNode plan = planner.createPlan(qc, context).getRootBlock().getRoot();
    testCloneLogicalNode(plan);

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    assertEquals(NodeType.CREATE_TABLE, root.getChild().getType());
    StoreTableNode storeNode = root.getChild();
    testQuery7(storeNode.getChild());
  }

  @Test
  public final void testOrderBy() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[4]);

    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    testCloneLogicalNode(plan);

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SORT, projNode.getChild().getType());
    SortNode sortNode = projNode.getChild();

    assertEquals(NodeType.JOIN, sortNode.getChild().getType());
    JoinNode joinNode = sortNode.getChild();

    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType());
    ScanNode leftNode = joinNode.getLeftChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), leftNode.getTableName());
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
    ScanNode rightNode = joinNode.getRightChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), rightNode.getTableName());
  }

  @Test
  public final void testLimit() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[12]);

    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    testCloneLogicalNode(plan);

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.LIMIT, projNode.getChild().getType());
    LimitNode limitNode = projNode.getChild();

    assertEquals(NodeType.SORT, limitNode.getChild().getType());
  }

  @Test
  public final void testSPJPush() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[5]);
    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    testCloneLogicalNode(plan);

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = projNode.getChild();
    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), scanNode.getTableName());
  }



  @Test
  public final void testSPJ() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[6]);
    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    testCloneLogicalNode(plan);
  }

  @Test
  public final void testVisitor() throws TajoException {
    QueryContext qc = createQueryContext();

    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[1]);
    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();

    TestVisitor vis = new TestVisitor();
    plan.postOrder(vis);

    assertEquals(NodeType.ROOT, vis.stack.pop().getType());
    assertEquals(NodeType.PROJECTION, vis.stack.pop().getType());
    assertEquals(NodeType.JOIN, vis.stack.pop().getType());
    assertEquals(NodeType.SCAN, vis.stack.pop().getType());
    assertEquals(NodeType.SCAN, vis.stack.pop().getType());
  }

  private static class TestVisitor implements LogicalNodeVisitor {
    Stack<LogicalNode> stack = new Stack<>();
    @Override
    public void visit(LogicalNode node) {
      stack.push(node);
    }
  }


  @Test
  public final void testExprNode() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[10]);
    LogicalPlan rootNode = planner.createPlan(qc, expr);
    LogicalNode plan = rootNode.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.EXPRS, root.getChild().getType());
    Schema out = root.getOutSchema();

    Iterator<Column> it = out.getRootColumns().iterator();
    Column col = it.next();
    assertEquals("res1", col.getSimpleName());
    col = it.next();
    assertEquals("res2", col.getSimpleName());
    col = it.next();
    assertEquals("res3", col.getSimpleName());
  }

  @Test
  public final void testCreateIndexNode() throws TajoException {
    QueryContext qc = new QueryContext(util.getConfiguration(), session);
    Expr expr = sqlAnalyzer.parse(QUERIES[11]);
    LogicalPlan rootNode = planner.createPlan(qc, expr);
    LogicalNode plan = rootNode.getRootBlock().getRoot();

    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.CREATE_INDEX, root.getChild().getType());
    CreateIndexNode createIndexNode = root.getChild();

    assertEquals(NodeType.PROJECTION, createIndexNode.getChild().getType());
    ProjectionNode projNode = createIndexNode.getChild();

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = projNode.getChild();

    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), scanNode.getTableName());
  }

  @Test
  public final void testAsterisk() throws CloneNotSupportedException, TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(QUERIES[13]);
    LogicalPlan planNode = planner.createPlan(qc, expr);
    LogicalNode plan = planNode.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    testCloneLogicalNode(plan);
    LogicalRootNode root = (LogicalRootNode) plan;

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = root.getChild();
    assertEquals(6, projNode.getOutSchema().size());

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = projNode.getChild();

    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), scanNode.getTableName());
  }

  static final String ALIAS [] = {
    "select deptName, sum(score) as total from score group by deptName",
    "select em.empId as id, sum(score) as total from employee as em inner join score using (em.deptName) group by id"
  };


  @Test
  public final void testAlias1() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(ALIAS[0]);
    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    LogicalRootNode root = (LogicalRootNode) plan;

    Schema finalSchema = root.getOutSchema();
    Iterator<Column> it = finalSchema.getRootColumns().iterator();
    Column col = it.next();
    assertEquals("deptname", col.getSimpleName());
    col = it.next();
    assertEquals("total", col.getSimpleName());

    expr = sqlAnalyzer.parse(ALIAS[1]);
    plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    root = (LogicalRootNode) plan;

    finalSchema = root.getOutSchema();
    it = finalSchema.getRootColumns().iterator();
    col = it.next();
    assertEquals("id", col.getSimpleName());
    col = it.next();
    assertEquals("total", col.getSimpleName());
  }

  @Test
  public final void testAlias2() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(ALIAS[1]);
    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    LogicalRootNode root = (LogicalRootNode) plan;

    Schema finalSchema = root.getOutSchema();
    Iterator<Column> it = finalSchema.getRootColumns().iterator();
    Column col = it.next();
    assertEquals("id", col.getSimpleName());
    col = it.next();
    assertEquals("total", col.getSimpleName());
  }

  static final String CREATE_TABLE [] = {
    "create external table table1 (name text, age int, earn bigint, score real) using text with ('text.delimiter'='|') location '/tmp/data'"
  };

  @Test
  public final void testCreateTableDef() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(CREATE_TABLE[0]);
    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.CREATE_TABLE, root.getChild().getType());
    CreateTableNode createTable = root.getChild();

    Schema def = createTable.getTableSchema();
    assertEquals("name", def.getColumn(0).getSimpleName());
    assertEquals(Type.TEXT, def.getColumn(0).getDataType().getType());
    assertEquals("age", def.getColumn(1).getSimpleName());
    assertEquals(Type.INT4, def.getColumn(1).getDataType().getType());
    assertEquals("earn", def.getColumn(2).getSimpleName());
    assertEquals(Type.INT8, def.getColumn(2).getDataType().getType());
    assertEquals("score", def.getColumn(3).getSimpleName());
    assertEquals(Type.FLOAT4, def.getColumn(3).getDataType().getType());
    assertTrue("TEXT".equalsIgnoreCase(createTable.getStorageType()));
    assertEquals("file://tmp/data", createTable.getUri().toString());
    assertTrue(createTable.hasOptions());
    assertEquals("\\u007c", createTable.getOptions().get("text.delimiter"));
  }

  private static final List<Set<Column>> testGenerateCuboidsResult
    = Lists.newArrayList();
  private static final int numCubeColumns = 3;
  private static final Column [] testGenerateCuboids = new Column[numCubeColumns];

  private static final List<Set<Column>> testCubeByResult
    = Lists.newArrayList();
  private static final Column [] testCubeByCuboids = new Column[2];
  static {
    testGenerateCuboids[0] = new Column("col1", Type.INT4);
    testGenerateCuboids[1] = new Column("col2", Type.INT8);
    testGenerateCuboids[2] = new Column("col3", Type.FLOAT4);

    testGenerateCuboidsResult.add(new HashSet<>());
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[1]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[2]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0],
        testGenerateCuboids[1]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0],
        testGenerateCuboids[2]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[1],
        testGenerateCuboids[2]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0],
        testGenerateCuboids[1], testGenerateCuboids[2]));

    testCubeByCuboids[0] = new Column("employee.name", Type.TEXT);
    testCubeByCuboids[1] = new Column("employee.empid", Type.INT4);
    testCubeByResult.add(new HashSet<>());
    testCubeByResult.add(Sets.newHashSet(testCubeByCuboids[0]));
    testCubeByResult.add(Sets.newHashSet(testCubeByCuboids[1]));
    testCubeByResult.add(Sets.newHashSet(testCubeByCuboids[0],
        testCubeByCuboids[1]));
  }

  @Test
  public final void testGenerateCuboids() {
    Column [] columns = new Column[3];

    columns[0] = new Column("col1", Type.INT4);
    columns[1] = new Column("col2", Type.INT8);
    columns[2] = new Column("col3", Type.FLOAT4);

    List<Column[]> cube = LogicalPlanner.generateCuboids(columns);
    assertEquals(((int)Math.pow(2, numCubeColumns)), cube.size());

    Set<Set<Column>> cuboids = Sets.newHashSet();
    for (Column [] cols : cube) {
      cuboids.add(Sets.newHashSet(cols));
    }

    for (Set<Column> result : testGenerateCuboidsResult) {
      assertTrue(cuboids.contains(result));
    }
  }

  static final String setStatements [] = {
    "select deptName from employee where deptName like 'data%' union all select deptName from score where deptName like 'data%'",
  };

  @Test
  public final void testSetPlan() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(setStatements[0]);
    LogicalNode plan = planner.createPlan(qc, expr).getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.UNION, root.getChild().getType());
    UnionNode union = root.getChild();
    assertEquals(NodeType.PROJECTION, union.getLeftChild().getType());
    assertEquals(NodeType.PROJECTION, union.getRightChild().getType());
  }

  static final String [] setQualifiers = {
    "select name, empid from employee",
    "select distinct name, empid from employee",
    "select all name, empid from employee",
  };

  @Test
  public void testSetQualifier() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr context = sqlAnalyzer.parse(setQualifiers[0]);
    LogicalNode plan = planner.createPlan(qc, context).getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projectionNode = root.getChild();
    assertEquals(NodeType.SCAN, projectionNode.getChild().getType());

    context = sqlAnalyzer.parse(setQualifiers[1]);
    plan = planner.createPlan(qc, context).getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    projectionNode = root.getChild();
    assertEquals(NodeType.GROUP_BY, projectionNode.getChild().getType());

    context = sqlAnalyzer.parse(setQualifiers[2]);
    plan = planner.createPlan(qc, context).getRootBlock().getRoot();
    root = (LogicalRootNode) plan;
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    projectionNode = root.getChild();
    assertEquals(NodeType.SCAN, projectionNode.getChild().getType());
  }

  // Table descriptions
  //
  // employee (name text, empid int4, deptname text)
  // dept (deptname text, manager text)
  // score (deptname text, score inet4)

  static final String [] insertStatements = {
      "insert into score select name from employee",                        // 0
      "insert into score select name, empid from employee",                 // 1
      "insert into employee (name, deptname) select * from dept",           // 2
      "insert into location '/tmp/data' select name, empid from employee",  // 3
      "insert overwrite into employee (name, deptname) select * from dept", // 4
      "insert overwrite into LOCATION '/tmp/data' select * from dept",      // 5
      "insert into employee (deptname, name) select deptname, manager from dept"  // 6
  };

  @Test
  public final void testInsertInto0() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(insertStatements[0]);
    LogicalPlan plan = planner.createPlan(qc, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertFalse(insertNode.isOverwrite());
    assertTrue(insertNode.hasTargetTable());
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), insertNode.getTableName());
  }

  @Test
  public final void testInsertInto1() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(insertStatements[1]);
    LogicalPlan plan = planner.createPlan(qc, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertFalse(insertNode.isOverwrite());
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), insertNode.getTableName());
  }

  @Test
  public final void testInsertInto2() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(insertStatements[2]);
    LogicalPlan plan = planner.createPlan(qc, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertFalse(insertNode.isOverwrite());
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), insertNode.getTableName());
    assertTrue(insertNode.hasTargetSchema());
    assertEquals(insertNode.getTargetSchema().getColumn(0).getSimpleName(), "name");
    assertEquals(insertNode.getTargetSchema().getColumn(1).getSimpleName(), "deptname");
  }

  @Test
  public final void testInsertInto3() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(insertStatements[3]);
    LogicalPlan plan = planner.createPlan(qc, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertFalse(insertNode.isOverwrite());
    assertTrue(insertNode.hasUri());
  }

  @Test
  public final void testInsertInto4() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(insertStatements[4]);
    LogicalPlan plan = planner.createPlan(qc, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertTrue(insertNode.isOverwrite());
    assertTrue(insertNode.hasTargetTable());
    assertEquals(IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), insertNode.getTableName());
    assertTrue(insertNode.hasTargetSchema());
    assertEquals(insertNode.getTargetSchema().getColumn(0).getSimpleName(), "name");
    assertEquals(insertNode.getTargetSchema().getColumn(1).getSimpleName(), "deptname");
  }

  @Test
  public final void testInsertInto5() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(insertStatements[5]);
    LogicalPlan plan = planner.createPlan(qc, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertTrue(insertNode.isOverwrite());
    assertTrue(insertNode.hasUri());
  }

  @Test
  public final void testInsertInto6() throws TajoException {
    QueryContext qc = createQueryContext();

    Expr expr = sqlAnalyzer.parse(insertStatements[6]);
    LogicalPlan plan = planner.createPlan(qc, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);

    ProjectionNode subquery = insertNode.getChild();
    List<Target> targets = subquery.getTargets();
    // targets MUST be manager, NULL as empid, deptname
    assertEquals(targets.get(0).getNamedColumn().getQualifiedName(), "default.dept.manager");
    assertEquals(targets.get(1).getAlias(), "empid");
    assertEquals(targets.get(1).getEvalTree().getType(), EvalType.CONST);
    assertEquals(targets.get(2).getNamedColumn().getQualifiedName(), "default.dept.deptname");
  }

  private static InsertNode getInsertNode(LogicalPlan plan) {
    LogicalRootNode root = plan.getRootBlock().getRoot();
    assertEquals(NodeType.INSERT, root.getChild().getType());
    return root.getChild();
  }

  @Test
  public final void testAlterTableRepairPartiton() throws TajoException {
    QueryContext qc = createQueryContext();

    String sql = "ALTER TABLE table1 REPAIR PARTITION";
    Expr expr = sqlAnalyzer.parse(sql);
    LogicalPlan rootNode = planner.createPlan(qc, expr);
    LogicalNode plan = rootNode.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.ALTER_TABLE, root.getChild().getType());

    AlterTableNode msckNode = root.getChild();

    assertEquals(msckNode.getAlterTableOpType(), AlterTableOpType.REPAIR_PARTITION);
    assertEquals(msckNode.getTableName(), "table1");
  }

  String [] ALTER_PARTITIONS = {
    "ALTER TABLE partitioned_table ADD PARTITION (col1 = 1 , col2 = 2) LOCATION 'hdfs://xxx" +
      ".com/warehouse/partitioned_table/col1=1/col2=2'", //0
    "ALTER TABLE partitioned_table DROP PARTITION (col1 = '2015' , col2 = '01', col3 = '11' )", //1
  };

  // TODO: This should be added at TAJO-1891
  public final void testAddPartitionAndDropPartition() throws TajoException {
    String tableName = IdentifierUtil.normalizeIdentifier("partitioned_table");
    String qualifiedTableName = IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME, tableName);

    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("name", Type.TEXT)
        .add("age", Type.INT4)
        .add("score", Type.FLOAT8)
        .build();

    KeyValueSet opts = new KeyValueSet();
    opts.set("file.delimiter", ",");

    Schema partSchema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("name", Type.TEXT)
        .build();

    PartitionMethodDesc partitionMethodDesc =
      new PartitionMethodDesc(DEFAULT_DATABASE_NAME, tableName,
        CatalogProtos.PartitionType.COLUMN, "id,name", partSchema);

    TableDesc desc = null;

    try {
      desc = new TableDesc(qualifiedTableName, schema, "TEXT", new KeyValueSet(),
        CommonTestingUtil.getTestDir().toUri());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    desc.setPartitionMethod(partitionMethodDesc);
    assertFalse(catalog.existsTable(qualifiedTableName));
    catalog.createTable(desc);
    assertTrue(catalog.existsTable(qualifiedTableName));

    TableDesc retrieved = catalog.getTableDesc(qualifiedTableName);
    assertEquals(retrieved.getName(), qualifiedTableName);
    assertEquals(retrieved.getPartitionMethod().getPartitionType(), CatalogProtos.PartitionType.COLUMN);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName(), "id");

    QueryContext qc = new QueryContext(util.getConfiguration(), session);

    // Testing alter table add partition
    Expr expr = sqlAnalyzer.parse(ALTER_PARTITIONS[0]);
    LogicalPlan rootNode = planner.createPlan(qc, expr);
    LogicalNode plan = rootNode.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.ALTER_TABLE, root.getChild().getType());

    AlterTableNode alterTableNode = root.getChild();

    assertEquals(alterTableNode.getAlterTableOpType(), AlterTableOpType.ADD_PARTITION);

    assertEquals(alterTableNode.getPartitionColumns().length, 2);
    assertEquals(alterTableNode.getPartitionValues().length, 2);

    assertEquals(alterTableNode.getPartitionColumns()[0], "col1");
    assertEquals(alterTableNode.getPartitionColumns()[1], "col2");

    assertEquals(alterTableNode.getPartitionValues()[0], "1");
    assertEquals(alterTableNode.getPartitionValues()[1], "2");

    assertEquals(alterTableNode.getLocation(), "hdfs://xxx.com/warehouse/partitioned_table/col1=1/col2=2");

    // Testing alter table drop partition
    expr = sqlAnalyzer.parse(ALTER_PARTITIONS[1]);
    rootNode = planner.createPlan(qc, expr);
    plan = rootNode.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;
    assertEquals(NodeType.ALTER_TABLE, root.getChild().getType());

    alterTableNode = root.getChild();

    assertEquals(alterTableNode.getAlterTableOpType(), AlterTableOpType.DROP_PARTITION);

    assertEquals(alterTableNode.getPartitionColumns().length, 3);
    assertEquals(alterTableNode.getPartitionValues().length, 3);

    assertEquals(alterTableNode.getPartitionColumns()[0], "col1");
    assertEquals(alterTableNode.getPartitionColumns()[1], "col2");
    assertEquals(alterTableNode.getPartitionColumns()[2], "col3");

    assertEquals(alterTableNode.getPartitionValues()[0], "2015");
    assertEquals(alterTableNode.getPartitionValues()[1], "01");
    assertEquals(alterTableNode.getPartitionValues()[2], "11");
  }

  String[] SELF_DESC = {
      "select id, name, dept from default.self_desc_table1", // 0
      "select name, dept from default.self_desc_table1 where id > 10",
  };

  @Test
  public void testSelectFromSelfDescTable() throws Exception {
    TableDesc tableDesc = new TableDesc("default.self_desc_table1", null,
        CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration()),
        CommonTestingUtil.getTestDir().toUri(), true);
    catalog.createTable(tableDesc);
    assertTrue(catalog.existsTable("default.self_desc_table1"));
    tableDesc = catalog.getTableDesc("default.self_desc_table1");
    assertTrue(tableDesc.hasEmptySchema());

    QueryContext context = createQueryContext();
    Expr expr = sqlAnalyzer.parse(SELF_DESC[0]);
    LogicalPlan logicalPlan = planner.createPlan(context, expr);

    LogicalNode node = logicalPlan.getRootNode();
    assertEquals(NodeType.ROOT, node.getType());
    LogicalRootNode root = (LogicalRootNode) node;
    testCloneLogicalNode(root);

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projectionNode = root.getChild();
    testCloneLogicalNode(projectionNode);

    // projection column test
    List<Target> targets = projectionNode.getTargets();
    Collections.sort(targets, new Comparator<Target>() {
      @Override
      public int compare(Target o1, Target o2) {
        return o1.getCanonicalName().compareTo(o2.getCanonicalName());
      }
    });
    assertEquals(3, targets.size());
    assertEquals("default.self_desc_table1.dept", targets.get(0).getCanonicalName());
    assertEquals("default.self_desc_table1.id", targets.get(1).getCanonicalName());
    assertEquals("default.self_desc_table1.name", targets.get(2).getCanonicalName());

    // scan column test
    assertEquals(NodeType.SCAN, projectionNode.getChild().getType());
    ScanNode scanNode = projectionNode.getChild();
    targets = scanNode.getTargets();
    Collections.sort(targets, new Comparator<Target>() {
      @Override
      public int compare(Target o1, Target o2) {
        return o1.getCanonicalName().compareTo(o2.getCanonicalName());
      }
    });
    assertEquals(3, targets.size());
    assertEquals("default.self_desc_table1.dept", targets.get(0).getCanonicalName());
    assertEquals("default.self_desc_table1.id", targets.get(1).getCanonicalName());
    assertEquals("default.self_desc_table1.name", targets.get(2).getCanonicalName());

    catalog.dropTable("default.self_desc_table1");
  }

  @Test
  public void testSelectWhereFromSelfDescTable() throws Exception {
    TableDesc tableDesc = new TableDesc("default.self_desc_table1", null,
        CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration()),
        CommonTestingUtil.getTestDir().toUri(), true);
    catalog.createTable(tableDesc);
    assertTrue(catalog.existsTable("default.self_desc_table1"));
    tableDesc = catalog.getTableDesc("default.self_desc_table1");
    assertTrue(tableDesc.hasEmptySchema());

    QueryContext context = createQueryContext();
    Expr expr = sqlAnalyzer.parse(SELF_DESC[1]);
    LogicalPlan logicalPlan = planner.createPlan(context, expr);

    LogicalNode node = logicalPlan.getRootNode();
    assertEquals(NodeType.ROOT, node.getType());
    LogicalRootNode root = (LogicalRootNode) node;
    testCloneLogicalNode(root);

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projectionNode = root.getChild();
    testCloneLogicalNode(projectionNode);

    // projection column test
    List<Target> targets = projectionNode.getTargets();
    Collections.sort(targets, new Comparator<Target>() {
      @Override
      public int compare(Target o1, Target o2) {
        return o1.getCanonicalName().compareTo(o2.getCanonicalName());
      }
    });
    assertEquals(2, targets.size());
    assertEquals("default.self_desc_table1.dept", targets.get(0).getCanonicalName());
    assertEquals("default.self_desc_table1.name", targets.get(1).getCanonicalName());

    assertEquals(NodeType.SELECTION, projectionNode.getChild().getType());
    SelectionNode selectionNode = projectionNode.getChild();
    assertEquals(new BinaryEval(EvalType.GTH, new FieldEval("default.self_desc_table1.id", CatalogUtil.newSimpleDataType(Type.TEXT)), new ConstEval(new Int4Datum(10))),
        selectionNode.getQual());

    // scan column test
    assertEquals(NodeType.SCAN, selectionNode.getChild().getType());
    ScanNode scanNode = selectionNode.getChild();
    targets = scanNode.getTargets();
    Collections.sort(targets, new Comparator<Target>() {
      @Override
      public int compare(Target o1, Target o2) {
        return o1.getCanonicalName().compareTo(o2.getCanonicalName());
      }
    });
    assertEquals(4, targets.size());
    assertEquals("?greaterthan", targets.get(0).getCanonicalName());
    assertEquals("default.self_desc_table1.dept", targets.get(1).getCanonicalName());
    assertEquals("default.self_desc_table1.id", targets.get(2).getCanonicalName());
    assertEquals("default.self_desc_table1.name", targets.get(3).getCanonicalName());

    catalog.dropTable("default.self_desc_table1");
  }
}
