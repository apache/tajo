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

import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tajo.*;
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
import org.apache.tajo.engine.function.hiveudf.HiveFunctionLoader;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.master.exec.QueryExecutor;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalContext;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.plan.serder.EvalNodeDeserializer;
import org.apache.tajo.plan.serder.EvalNodeSerializer;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.plan.verifier.LogicalPlanVerifier;
import org.apache.tajo.plan.verifier.PreLogicalPlanVerifier;
import org.apache.tajo.plan.verifier.VerificationState;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.text.CSVLineSerDe;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.*;

public class ExprTestBase {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static CatalogService cat;
  private static SQLAnalyzer analyzer;
  private static PreLogicalPlanVerifier preLogicalPlanVerifier;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static LogicalPlanVerifier annotatedPlanVerifier;

  public static String getUserTimeZoneDisplay(TimeZone tz) {
    return DateTimeUtil.getDisplayTimeZoneOffset(tz, false);
  }

  public ExprTestBase() {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new TajoTestingCluster();
    conf = cluster.getConfiguration();
    cluster.startCatalogCluster();
    cat = cluster.getCatalogService();
    cat.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    cat.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    Map<FunctionSignature, FunctionDesc> map = FunctionLoader.loadBuiltinFunctions();
    List<FunctionDesc> list = new ArrayList<>(map.values());
    list.addAll(FunctionLoader.loadUserDefinedFunctions(conf).orElse(new ArrayList<>()));

    // load Hive UDFs
    URL hiveUDFURL = ClassLoader.getSystemResource("hiveudf");
    Preconditions.checkNotNull(hiveUDFURL, "hive udf directory is absent.");
    conf.set(TajoConf.ConfVars.HIVE_UDF_JAR_DIR.varname, hiveUDFURL.toString().substring("file:".length()));
    list.addAll(HiveFunctionLoader.loadHiveUDFs(conf).orElse(new ArrayList<>()));

    for (FunctionDesc funcDesc : list) {
      cat.createFunction(funcDesc);
    }

    analyzer = new SQLAnalyzer();
    preLogicalPlanVerifier = new PreLogicalPlanVerifier(cat);
    planner = new LogicalPlanner(cat, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(cluster.getConfiguration(), cat, TablespaceManager.getInstance());
    annotatedPlanVerifier = new LogicalPlanVerifier();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.shutdownCatalogCluster();
  }

  public TajoConf getConf() {
    return new TajoConf(conf);
  }

  protected TajoTestingCluster getCluster() {
    return cluster;
  }

  /**
   * verify query syntax and get raw targets.
   *
   * @param context QueryContext
   * @param query a query for execution
   * @param condition this parameter means whether it is for success case or is not for failure case.
   * @return
   */
  private static List<Target> getRawTargets(QueryContext context, String query, boolean condition)
      throws TajoException, InvalidStatementException {

    List<ParsedResult> parsedResults = SimpleParser.parseScript(query);
    if (parsedResults.size() > 1) {
      throw new RuntimeException("this query includes two or more statements.");
    }
    Expr expr = analyzer.parse(parsedResults.get(0).getHistoryStatement());
    VerificationState state = new VerificationState();
    preLogicalPlanVerifier.verify(context, state, expr);
    if (state.getErrors().size() > 0) {
      if (!condition && state.getErrors().size() > 0) {
        throw new RuntimeException(state.getErrors().get(0));
      }
      assertFalse(state.getErrors().get(0).getMessage(), true);
    }
    LogicalPlan plan = planner.createPlan(context, expr, true);
    optimizer.optimize(context, plan);
    annotatedPlanVerifier.verify(state, plan);

    if (state.getErrors().size() > 0) {
      assertFalse(state.getErrors().get(0).getMessage(), true);
    }

    List<Target> targets = plan.getRootBlock().getRawTargets();
    if (targets == null) {
      throw new RuntimeException("Wrong query statement or query plan: " + parsedResults.get(0).getHistoryStatement());
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
      assertEvalTreeProtoSerDer(context, t.getEvalTree());
    }
    return targets;
  }

  public void testSimpleEval(String query, String [] expected) throws TajoException {
    testEval(null, null, null, query, expected);
  }

  public void testSimpleEval(OverridableConf context, String query, String [] expected) throws TajoException {
    testEval(context, null, null, null, query, expected);
  }

  public void testSimpleEval(String query, String [] expected, boolean successOrFail)
      throws TajoException, IOException {

    testEval(null, null, null, null, query, expected, ',', successOrFail);
  }

  public void testSimpleEval(OverridableConf context, String query, String [] expected, boolean successOrFail)
      throws TajoException, IOException {
    testEval(context, null, null, null, query, expected, ',', successOrFail);
  }

  public void testEval(Schema schema, String tableName, String csvTuple, String query, String [] expected)
      throws TajoException {
    testEval(null, schema, tableName != null ? IdentifierUtil.normalizeIdentifier(tableName) : null, csvTuple, query,
        expected, ',', true);
  }

  public void testEval(OverridableConf context, Schema schema, String tableName, String csvTuple, String query,
                       String [] expected)
      throws TajoException {
    testEval(context, schema, tableName != null ? IdentifierUtil.normalizeIdentifier(tableName) : null, csvTuple,
        query, expected, ',', true);
  }

  public void testEval(Schema schema, String tableName, String csvTuple, String query,
                       String [] expected, char delimiter, boolean condition) throws TajoException {
    testEval(null, schema, tableName != null ? IdentifierUtil.normalizeIdentifier(tableName) : null, csvTuple,
        query, expected, delimiter, condition);
  }

  public void testEval(OverridableConf context, Schema schema, String tableName, String csvTuple, String query,
                       String [] expected, char delimiter, boolean condition) throws TajoException {
    QueryContext queryContext;
    if (context == null) {
      queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    } else {
      queryContext = LocalTajoTestingUtility.createDummyContext(context.getConf());
      queryContext.putAll(context);
    }

    VTuple vtuple  = null;
    String qualifiedTableName =
        IdentifierUtil.buildFQName(DEFAULT_DATABASE_NAME,
            tableName != null ? IdentifierUtil.normalizeIdentifier(tableName) : null);
    Schema inputSchema = null;


    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, queryContext.getConf());
    meta.putProperty(StorageConstants.TEXT_DELIMITER, StringEscapeUtils.escapeJava(delimiter+""));
    meta.putProperty(StorageConstants.TEXT_NULL, StringEscapeUtils.escapeJava("\\NULL"));

    String timezoneId = queryContext.get(SessionVars.TIMEZONE);
    TimeZone timeZone = TimeZone.getTimeZone(timezoneId);

    if (schema != null) {
      inputSchema = SchemaUtil.clone(schema);
      inputSchema.setQualifier(qualifiedTableName);

      try {
        cat.createTable(CatalogUtil.newTableDesc(
            qualifiedTableName, inputSchema, meta, CommonTestingUtil.getTestDir()));
      } catch (IOException e) {
        throw new TajoInternalError(e);
      }

      CSVLineSerDe serDe = new CSVLineSerDe();
      TextLineDeserializer deserializer = serDe.createDeserializer(inputSchema, meta, inputSchema.toArray());
      deserializer.init();

      vtuple = new VTuple(inputSchema.size());

      try {
        deserializer.deserialize(Unpooled.wrappedBuffer(csvTuple.getBytes()), vtuple);
      } catch (Exception e) {
        throw new TajoInternalError(e);
      } finally {
        deserializer.release();
      }
    }

    List<Target> targets;

    TajoClassLoader classLoader = new TajoClassLoader();
    EvalContext evalContext = new EvalContext();
    evalContext.setTimeZone(timeZone);

    try {
      if (needPythonFileCopy()) {
        PythonScriptEngine.initPythonScriptEngineFiles();
      }
      targets = getRawTargets(queryContext, query, condition);

      EvalCodeGenerator codegen = null;
      if (queryContext.getBool(SessionVars.CODEGEN)) {
        codegen = new EvalCodeGenerator(classLoader);
      }

      QueryExecutor.startScriptExecutors(queryContext, evalContext, targets);
      Tuple outTuple = new VTuple(targets.size());
      for (int i = 0; i < targets.size(); i++) {
        EvalNode eval = targets.get(i).getEvalTree();

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
        } else {
          outTupleAsChars = outTuple.asDatum(i).toString();
        }
        assertEquals(query, expected[i], outTupleAsChars);
      }
    } catch (IOException e) {
      throw new TajoInternalError(e);
    } catch (InvalidStatementException e) {
      assertFalse(e.getMessage(), true);
    } catch (TajoException e) {
      // In failure test case, an exception must occur while executing query.
      // So, we should check an error message, and return it.
      if (!condition) {
        assertEquals(expected[0], e.getMessage());
      } else {
        throw e;
      }
    } finally {
      if (schema != null) {
        cat.dropTable(qualifiedTableName);
      }
      QueryExecutor.stopScriptExecutors(evalContext);
    }
  }

  private static boolean needPythonFileCopy() {
    File contoller = new File(PythonScriptEngine.getControllerPath());
    return !contoller.exists();
  }

  public static void assertEvalTreeProtoSerDer(OverridableConf context, EvalNode evalNode) {
    PlanProto.EvalNodeTree converted = EvalNodeSerializer.serialize(evalNode);
    assertEquals(evalNode, EvalNodeDeserializer.deserialize(context, null, converted));
  }
}
