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

package org.apache.tajo.engine.eval;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.cli.tsql.InvalidStatementException;
import org.apache.tajo.cli.tsql.ParsedResult;
import org.apache.tajo.cli.tsql.SimpleParser;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.codegen.EvalCodeGenerator;
import org.apache.tajo.engine.codegen.TajoClassLoader;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.master.exec.QueryExecutor;
import org.apache.tajo.plan.*;
import org.apache.tajo.plan.expr.EvalContext;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.serder.EvalNodeDeserializer;
import org.apache.tajo.plan.serder.EvalNodeSerializer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.plan.verifier.LogicalPlanVerifier;
import org.apache.tajo.plan.verifier.PreLogicalPlanVerifier;
import org.apache.tajo.plan.verifier.VerificationState;
import org.apache.tajo.storage.LazyTuple;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.BytesUtils;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ExprTestBase {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService cat;
  private static SQLAnalyzer analyzer;
  private static PreLogicalPlanVerifier preLogicalPlanVerifier;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static LogicalPlanVerifier annotatedPlanVerifier;

  public static String getUserTimeZoneDisplay(TimeZone tz) {
    return DateTimeUtil.getTimeZoneDisplayTime(tz);
  }

  public ExprTestBase() {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    conf = util.getConfiguration();
    util.startCatalogCluster();
    cat = util.getMiniCatalogCluster().getCatalog();
    cat.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    cat.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    Map<FunctionSignature, FunctionDesc> map = FunctionLoader.load();
    map = FunctionLoader.loadUserDefinedFunctions(conf, map);
    for (FunctionDesc funcDesc : map.values()) {
      cat.createFunction(funcDesc);
    }

    analyzer = new SQLAnalyzer();
    preLogicalPlanVerifier = new PreLogicalPlanVerifier(cat);
    planner = new LogicalPlanner(cat, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(util.getConfiguration());
    annotatedPlanVerifier = new LogicalPlanVerifier(util.getConfiguration(), cat);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  private static void assertJsonSerDer(EvalNode expr) {
    String json = CoreGsonHelper.toJson(expr, EvalNode.class);
    EvalNode fromJson = CoreGsonHelper.fromJson(json, EvalNode.class);
    assertEquals(expr, fromJson);
  }

  public TajoConf getConf() {
    return new TajoConf(conf);
  }

  /**
   * verify query syntax and get raw targets.
   *
   * @param context QueryContext
   * @param query a query for execution
   * @param condition this parameter means whether it is for success case or is not for failure case.
   * @return
   * @throws PlanningException
   */
  private static Target[] getRawTargets(QueryContext context, String query, boolean condition)
      throws PlanningException, InvalidStatementException {

    List<ParsedResult> parsedResults = SimpleParser.parseScript(query);
    if (parsedResults.size() > 1) {
      throw new RuntimeException("this query includes two or more statements.");
    }
    Expr expr = analyzer.parse(parsedResults.get(0).getHistoryStatement());
    VerificationState state = new VerificationState();
    preLogicalPlanVerifier.verify(context, state, expr);
    if (state.getErrorMessages().size() > 0) {
      if (!condition && state.getErrorMessages().size() > 0) {
        throw new PlanningException(state.getErrorMessages().get(0));
      }
      assertFalse(state.getErrorMessages().get(0), true);
    }
    LogicalPlan plan = planner.createPlan(context, expr, true);
    optimizer.optimize(context, plan);
    annotatedPlanVerifier.verify(context, state, plan);

    if (state.getErrorMessages().size() > 0) {
      assertFalse(state.getErrorMessages().get(0), true);
    }

    Target [] targets = plan.getRootBlock().getRawTargets();
    if (targets == null) {
      throw new PlanningException("Wrong query statement or query plan: " + parsedResults.get(0).getHistoryStatement());
    }

    // Trying regression test for cloning, (de)serialization for json and protocol buffer
    for (Target t : targets) {
      try {
        assertEquals(t.getEvalTree(), t.getEvalTree().clone());
      } catch (CloneNotSupportedException e) {
        fail(e.getMessage());
      }
    }
    for (Target t : targets) {
      assertJsonSerDer(t.getEvalTree());
    }
    for (Target t : targets) {
      assertEvalTreeProtoSerDer(context, t.getEvalTree());
    }
    return targets;
  }

  public void testSimpleEval(String query, String [] expected) throws IOException {
    testEval(null, null, null, query, expected);
  }

  public void testSimpleEval(OverridableConf context, String query, String [] expected) throws IOException {
    testEval(context, null, null, null, query, expected);
  }

  public void testSimpleEval(String query, String [] expected, boolean successOrFail)
      throws IOException {

    testEval(null, null, null, null, query, expected, ',', successOrFail);
  }

  public void testSimpleEval(OverridableConf context, String query, String [] expected, boolean successOrFail)
      throws IOException {
    testEval(context, null, null, null, query, expected, ',', successOrFail);
  }

  public void testEval(Schema schema, String tableName, String csvTuple, String query, String [] expected)
      throws IOException {
    testEval(null, schema, tableName != null ? CatalogUtil.normalizeIdentifier(tableName) : null, csvTuple, query,
        expected, ',', true);
  }

  public void testEval(OverridableConf context, Schema schema, String tableName, String csvTuple, String query,
                       String [] expected)
      throws IOException {
    testEval(context, schema, tableName != null ? CatalogUtil.normalizeIdentifier(tableName) : null, csvTuple,
        query, expected, ',', true);
  }

  public void testEval(Schema schema, String tableName, String csvTuple, String query,
                       String [] expected, char delimiter, boolean condition) throws IOException {
    testEval(null, schema, tableName != null ? CatalogUtil.normalizeIdentifier(tableName) : null, csvTuple,
        query, expected, delimiter, condition);
  }

  public void testEval(OverridableConf context, Schema schema, String tableName, String csvTuple, String query,
                       String [] expected, char delimiter, boolean condition) throws IOException {
    QueryContext queryContext;
    if (context == null) {
      queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    } else {
      queryContext = LocalTajoTestingUtility.createDummyContext(conf);
      queryContext.putAll(context);
    }

    String timezoneId = queryContext.get(SessionVars.TIMEZONE);
    TimeZone timeZone = TimeZone.getTimeZone(timezoneId);

    LazyTuple lazyTuple;
    VTuple vtuple  = null;
    String qualifiedTableName =
        CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME,
            tableName != null ? CatalogUtil.normalizeIdentifier(tableName) : null);
    Schema inputSchema = null;
    if (schema != null) {
      inputSchema = SchemaUtil.clone(schema);
      inputSchema.setQualifier(qualifiedTableName);

      int targetIdx [] = new int[inputSchema.size()];
      for (int i = 0; i < targetIdx.length; i++) {
        targetIdx[i] = i;
      }

      byte[][] tokens = BytesUtils.splitPreserveAllTokens(
          csvTuple.getBytes(), delimiter, targetIdx, inputSchema.size());
      lazyTuple = new LazyTuple(inputSchema, tokens,0);
      vtuple = new VTuple(inputSchema.size());
      for (int i = 0; i < inputSchema.size(); i++) {

        // If null value occurs, null datum is manually inserted to an input tuple.
        boolean nullDatum;
        Datum datum = lazyTuple.get(i);
        nullDatum = (datum instanceof TextDatum || datum instanceof CharDatum);
        nullDatum = nullDatum &&
            datum.asChars().equals("") || datum.asChars().equals(queryContext.get(SessionVars.NULL_CHAR));
        nullDatum |= datum.isNull();

        if (nullDatum) {
          vtuple.put(i, NullDatum.get());
        } else {
          vtuple.put(i, lazyTuple.get(i));
        }
      }
      cat.createTable(new TableDesc(qualifiedTableName, inputSchema,"CSV",
          new KeyValueSet(), CommonTestingUtil.getTestDir().toUri()));
    }

    Target [] targets;

    TajoClassLoader classLoader = new TajoClassLoader();
    EvalContext evalContext = new EvalContext();

    try {
      targets = getRawTargets(queryContext, query, condition);

      EvalCodeGenerator codegen = null;
      if (queryContext.getBool(SessionVars.CODEGEN)) {
        codegen = new EvalCodeGenerator(classLoader);
      }

      QueryExecutor.startScriptExecutors(queryContext, evalContext, targets);
      Tuple outTuple = new VTuple(targets.length);
      for (int i = 0; i < targets.length; i++) {
        EvalNode eval = targets[i].getEvalTree();

        if (queryContext.getBool(SessionVars.CODEGEN)) {
          eval = codegen.compile(inputSchema, eval);
        }
        eval.bind(evalContext, inputSchema);

        outTuple.put(i, eval.eval(vtuple));
      }

      try {
        classLoader.clean();
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }

      for (int i = 0; i < expected.length; i++) {
        String outTupleAsChars;
        if (outTuple.type(i) == Type.TIMESTAMP) {
          outTupleAsChars = TimestampDatum.asChars(outTuple.getTimeDate(i), timeZone, false);
        } else if (outTuple.type(i) == Type.TIME) {
          outTupleAsChars = TimeDatum.asChars(outTuple.getTimeDate(i), timeZone, false);
        } else {
          outTupleAsChars = outTuple.getText(i);
        }
        assertEquals(query, expected[i], outTupleAsChars);
      }
    } catch (InvalidStatementException e) {
      assertFalse(e.getMessage(), true);
    } catch (PlanningException e) {
      // In failure test case, an exception must occur while executing query.
      // So, we should check an error message, and return it.
      if (!condition) {
        assertEquals(expected[0], e.getMessage());
      } else {
        assertFalse(e.getMessage(), true);
      }
    } finally {
      if (schema != null) {
        cat.dropTable(qualifiedTableName);
      }
      QueryExecutor.stopScriptExecutors(evalContext);
    }
  }

  public static void assertEvalTreeProtoSerDer(OverridableConf context, EvalNode evalNode) {
    PlanProto.EvalNodeTree converted = EvalNodeSerializer.serialize(evalNode);
    assertEquals(evalNode, EvalNodeDeserializer.deserialize(context, null, converted));
  }
}
