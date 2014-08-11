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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.Selection;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.BooleanDatum;
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
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.*;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;

public class TestLogicalPlanConvertor {
  static TajoTestingCluster util;
  static CatalogService catalog = null;
  static SQLAnalyzer analyzer;
  static LogicalPlanner planner;
  static Session session = LocalTajoTestingUtility.createDummySession();

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);

    Schema schema = new Schema();
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    schema.addColumn("score", TajoDataTypes.Type.INT4);
    schema.addColumn("age", TajoDataTypes.Type.INT4);

    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
    TableDesc desc = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "people"), schema, meta,
        CommonTestingUtil.getTestDir());
    catalog.createTable(desc);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  public static Target[] getRawTargets(String query) {
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = null;
    try {
      plan = planner.createPlan(session, expr);
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

  }

  @Test
  public void testDatumConvert() throws Exception {
    assertSerializationDatum(DatumFactory.createBool(true));
    assertSerializationDatum(DatumFactory.createBool(false));
    assertSerializationDatum(DatumFactory.createInt2((short) 1));
    assertSerializationDatum(DatumFactory.createInt4(1980));
    assertSerializationDatum(DatumFactory.createInt8(19800401));
    assertSerializationDatum(DatumFactory.createFloat4(3.14f));
    assertSerializationDatum(DatumFactory.createFloat8(3.141592d));
    assertSerializationDatum(DatumFactory.createText("Apache Tajo"));
    assertSerializationDatum(DatumFactory.createBlob("Apache Tajo".getBytes()));
  }

  private static void assertSerializationDatum(Datum datum) {
    PlanProto.Datum converted = LogicalPlanConvertor.serialize(datum);
    assertEquals(datum, LogicalPlanConvertor.deserialize(converted));
  }

  @Test
  public void testConvertDatum() throws Exception {

  }
}
