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

package org.apache.tajo.engine.function;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.storage.LazyTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExprTestBase {
  private static TajoTestingCluster util;
  private static CatalogService cat;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    cat = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      cat.registerFunction(funcDesc);
    }

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(cat);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  private static void assertJsonSerDer(EvalNode expr) {
    String json = expr.toJson();
    EvalNode fromJson = CoreGsonHelper.fromJson(json, EvalNode.class);
    assertEquals(expr, fromJson);
  }

  private static Target[] getRawTargets(String query) throws PlanningException {
    Expr expr = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(expr);
    Target [] targets = plan.getRootBlock().getTargetListManager().getUnresolvedTargets();
    if (targets == null) {
      throw new PlanningException("Wrong query statement or query plan: " + query);
    }
    for (Target t : targets) {
      assertJsonSerDer(t.getEvalTree());
    }
    return targets;
  }

  public void testSimpleEval(String query, String [] expected) {
    testEval(null, null, null, query, expected);
  }

  public void testEval(Schema schema, String tableName, String csvTuple, String query, String [] expected) {
    LazyTuple lazyTuple;
    VTuple vtuple  = null;
    Schema inputSchema = null;
    if (schema != null) {
      inputSchema = (Schema) schema.clone();
      inputSchema.setQualifier(tableName, true);

      int targetIdx [] = new int[inputSchema.getColumnNum()];
      for (int i = 0; i < targetIdx.length; i++) {
        targetIdx[i] = i;
      }

      lazyTuple = new LazyTuple(inputSchema, Bytes.splitPreserveAllTokens(csvTuple.getBytes(), ',', targetIdx), 0);
      vtuple = new VTuple(inputSchema.getColumnNum());
      for (int i = 0; i < inputSchema.getColumnNum(); i++) {
        // If null value occurs, null datum is manually inserted to an input tuple.
        if (lazyTuple.get(i) instanceof TextDatum && lazyTuple.getText(i).asChars().equals("")) {
          vtuple.put(i, NullDatum.get());
        } else {
          vtuple.put(i, lazyTuple.get(i));
        }
      }
      cat.addTable(new TableDescImpl(tableName, inputSchema, CatalogProtos.StoreType.CSV, new Options(), new Path("/")));
    }

    Target [] targets = null;

    try {
      targets = getRawTargets(query);
    } catch (PlanningException e) {
      assertTrue("Wrong query statement: " + query, false);
    }

    EvalContext [] evalContexts = new EvalContext[targets.length];
    Tuple outTuple = new VTuple(targets.length);
    for (int i = 0; i < targets.length; i++) {
      EvalNode eval = targets[i].getEvalTree();
      evalContexts[i] = eval.newContext();
      eval.eval(evalContexts[i], inputSchema, vtuple);
      outTuple.put(i, eval.terminate(evalContexts[i]));
    }

    if (schema != null) {
      cat.deleteTable(tableName);
    }

    for (int i = 0; i < expected.length; i++) {
      assertEquals(query, expected[i], outTuple.get(i).asChars());
    }
  }
}
