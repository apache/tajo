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

package org.apache.tajo.engine.plan;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.Selection;
import org.apache.tajo.catalog.*;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.plan.proto.PlanProto;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.nameresolver.NameResolvingMode;
import org.apache.tajo.master.session.Session;
import org.junit.*;

import static org.junit.Assert.assertEquals;

public class TestLogicalPlanConvertor {
  static TajoTestingCluster util;
  static CatalogService catalog = null;
  static SQLAnalyzer analyzer;
  static LogicalPlanner planner;
  static Session session = LocalTajoTestingUtility.createDummySession();

  @BeforeClass
  public static void setUp() throws Exception {
    util = TpchTestBase.getInstance().getTestingCluster();
    catalog = util.getMaster().getCatalog();

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  public static Target[] getRawTargets(String query) {
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = null;
    try {
      plan = planner.createPlan(session, expr, true);
    } catch (PlanningException e) {
      e.printStackTrace();
    }

    return plan.getRootBlock().getRawTargets();
  }

  public static EvalNode getRootSelection(String query) throws PlanningException {
    Expr block = analyzer.parse(query);
    LogicalPlan plan = null;
    try {
      plan = planner.createPlan(session, block);
    } catch (PlanningException e) {
      e.printStackTrace();
    }

    Selection selection = plan.getRootBlock().getSingletonExpr(OpType.Filter);
    return planner.getExprAnnotator().createEvalNode(plan, plan.getRootBlock(), selection.getQual(),
        NameResolvingMode.RELS_AND_SUBEXPRS);
  }

  @Test
  public void testConvert() throws Exception {
    Target [] targets = getRawTargets("select 1 + 2");
    assertEvalNodeProtoSerder(targets[0].getEvalTree());

    targets = getRawTargets("select l_orderkey + l_partkey from lineitem");
    assertEvalNodeProtoSerder(targets[0].getEvalTree());
  }

  @Test
  public void testDatumConvert() throws Exception {
    assertDatumProtoSerder(DatumFactory.createBool(true));
    assertDatumProtoSerder(DatumFactory.createBool(false));
    assertDatumProtoSerder(DatumFactory.createInt2((short) 1));
    assertDatumProtoSerder(DatumFactory.createInt4(1980));
    assertDatumProtoSerder(DatumFactory.createInt8(19800401));
    assertDatumProtoSerder(DatumFactory.createFloat4(3.14f));
    assertDatumProtoSerder(DatumFactory.createFloat8(3.141592d));
    assertDatumProtoSerder(DatumFactory.createText("Apache Tajo"));
    assertDatumProtoSerder(DatumFactory.createBlob("Apache Tajo".getBytes()));
  }

  public static void assertDatumProtoSerder(Datum datum) {
    PlanProto.Datum converted = LogicalPlanConvertor.serialize(datum);
    assertEquals(datum, LogicalPlanConvertor.deserialize(converted));
  }

  public static void assertEvalNodeProtoSerder(EvalNode evalNode) {
    PlanProto.EvalTree converted = LogicalPlanConvertor.serialize(evalNode);
    assertEquals(evalNode, LogicalPlanConvertor.deserialize(converted));
  }

  @Test
  public void testConvertDatum() throws Exception {

  }
}
