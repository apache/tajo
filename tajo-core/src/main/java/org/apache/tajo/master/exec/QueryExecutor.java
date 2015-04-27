/*
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

package org.apache.tajo.master.exec;

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.physical.EvalExprExec;
import org.apache.tajo.engine.planner.physical.StoreTableExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;
import org.apache.tajo.master.*;
import org.apache.tajo.master.exec.prehook.CreateTableHook;
import org.apache.tajo.master.exec.prehook.DistributedQueryHookManager;
import org.apache.tajo.master.exec.prehook.InsertIntoHook;
import org.apache.tajo.plan.expr.EvalContext;
import org.apache.tajo.plan.expr.GeneralFunctionEval;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.plan.function.python.TajoScriptEngine;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.querymaster.*;
import org.apache.tajo.session.Session;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.verifier.VerifyException;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.ProtoUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueryExecutor {
  private static final Log LOG = LogFactory.getLog(QueryExecutor.class);

  private final TajoMaster.MasterContext context;
  private final CatalogService catalog;
  private final DistributedQueryHookManager hookManager;
  private final DDLExecutor ddlExecutor;

  public QueryExecutor(TajoMaster.MasterContext context, DDLExecutor ddlExecutor) {
    this.context = context;
    this.catalog = context.getCatalog();

    this.ddlExecutor = ddlExecutor;
    this.hookManager = new DistributedQueryHookManager();
    this.hookManager.addHook(new CreateTableHook());
    this.hookManager.addHook(new InsertIntoHook());
  }

  public SubmitQueryResponse execute(QueryContext queryContext, Session session, String sql, String jsonExpr,
                      LogicalPlan plan) throws Exception {

    SubmitQueryResponse.Builder response = SubmitQueryResponse.newBuilder();
    response.setIsForwarded(false);
    response.setUserName(queryContext.get(SessionVars.USERNAME));

    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    if (PlannerUtil.checkIfSetSession(rootNode)) {
      execSetSession(session, plan, response);


    } else if (PlannerUtil.checkIfDDLPlan(rootNode)) {
      context.getSystemMetrics().counter("Query", "numDDLQuery").inc();
      ddlExecutor.execute(queryContext, plan);
      response.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      response.setResultCode(ClientProtos.ResultCode.OK);


    } else if (plan.isExplain()) { // explain query
      execExplain(plan, queryContext, plan.isExplainGlobal(), response);

    } else if (PlannerUtil.checkIfQueryTargetIsVirtualTable(plan)) {
      execQueryOnVirtualTable(queryContext, session, sql, plan, response);

      // Simple query indicates a form of 'select * from tb_name [LIMIT X];'.
    } else if (PlannerUtil.checkIfSimpleQuery(plan)) {
      execSimpleQuery(queryContext, session, sql, plan, response);

      // NonFromQuery indicates a form of 'select a, x+y;'
    } else if (PlannerUtil.checkIfNonFromQuery(plan)) {
      execNonFromQuery(queryContext, plan, response);

    } else { // it requires distributed execution. So, the query is forwarded to a query master.
      executeDistributedQuery(queryContext, session, plan, sql, jsonExpr, response);
    }

    response.setSessionVars(ProtoUtil.convertFromMap(session.getAllVariables()));

    return response.build();
  }

  public void execSetSession(Session session, LogicalPlan plan,
                             SubmitQueryResponse.Builder response) {
    SetSessionNode setSessionNode = ((LogicalRootNode)plan.getRootBlock().getRoot()).getChild();

    final String varName = setSessionNode.getName();

    // SET CATALOG 'XXX'
    if (varName.equals(SessionVars.CURRENT_DATABASE.name())) {
      String databaseName = setSessionNode.getValue();

      if (catalog.existDatabase(databaseName)) {
        session.selectDatabase(setSessionNode.getValue());
      } else {
        response.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
        response.setResultCode(ClientProtos.ResultCode.ERROR);
        response.setErrorMessage("database \"" + databaseName + "\" does not exists.");
      }

      // others
    } else {
      if (setSessionNode.hasValue()) {
        session.setVariable(varName, setSessionNode.getValue());
      } else {
        session.removeVariable(varName);
      }
    }

    context.getSystemMetrics().counter("Query", "numDDLQuery").inc();
    response.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
    response.setResultCode(ClientProtos.ResultCode.OK);
  }

  public void execExplain(LogicalPlan plan, QueryContext queryContext, boolean isGlobal,
                          SubmitQueryResponse.Builder response)
      throws Exception {
    String explainStr;
    boolean isTest = queryContext.getBool(SessionVars.TEST_PLAN_SHAPE_FIX_ENABLED);
    if (isTest) {
      ExplainPlanPreprocessorForTest preprocessorForTest = new ExplainPlanPreprocessorForTest();
      preprocessorForTest.prepareTest(plan);
    }

    if (isGlobal) {
      GlobalPlanner planner = new GlobalPlanner(context.getConf(), context.getCatalog());
      MasterPlan masterPlan = compileMasterPlan(plan, queryContext, planner);
      if (isTest) {
        ExplainGlobalPlanPreprocessorForTest globalPlanPreprocessorForTest = new ExplainGlobalPlanPreprocessorForTest();
        globalPlanPreprocessorForTest.prepareTest(masterPlan);
      }
      explainStr = masterPlan.toString();
    } else {
      explainStr = PlannerUtil.buildExplainString(plan.getRootBlock().getRoot());
    }

    Schema schema = new Schema();
    schema.addColumn("explain", TajoDataTypes.Type.TEXT);
    RowStoreUtil.RowStoreEncoder encoder = RowStoreUtil.createEncoder(schema);

    ClientProtos.SerializedResultSet.Builder serializedResBuilder = ClientProtos.SerializedResultSet.newBuilder();

    VTuple tuple = new VTuple(1);
    String[] lines = explainStr.split("\n");
    int bytesNum = 0;
    for (String line : lines) {
      tuple.put(0, DatumFactory.createText(line));
      byte [] encodedData = encoder.toBytes(tuple);
      bytesNum += encodedData.length;
      serializedResBuilder.addSerializedTuples(ByteString.copyFrom(encodedData));
    }
    serializedResBuilder.setSchema(schema.getProto());
    serializedResBuilder.setBytesNum(bytesNum);

    response.setResultSet(serializedResBuilder.build());
    response.setMaxRowNum(lines.length);
    response.setResultCode(ClientProtos.ResultCode.OK);
    response.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
  }

  public void execQueryOnVirtualTable(QueryContext queryContext, Session session, String query, LogicalPlan plan,
                              SubmitQueryResponse.Builder response) throws Exception {
    int maxRow = Integer.MAX_VALUE;
    if (plan.getRootBlock().hasNode(NodeType.LIMIT)) {
      LimitNode limitNode = plan.getRootBlock().getNode(NodeType.LIMIT);
      maxRow = (int) limitNode.getFetchFirstNum();
    }
    QueryId queryId = QueryIdFactory.newQueryId(context.getResourceManager().getSeedQueryId());

    NonForwardQueryResultScanner queryResultScanner =
            new NonForwardQueryResultSystemScanner(context, plan, queryId, session.getSessionId(), maxRow);

    queryResultScanner.init();
    session.addNonForwardQueryResultScanner(queryResultScanner);

    response.setQueryId(queryId.getProto());
    response.setMaxRowNum(maxRow);
    response.setTableDesc(queryResultScanner.getTableDesc().getProto());
    response.setResultCode(ClientProtos.ResultCode.OK);
  }

  public void execSimpleQuery(QueryContext queryContext, Session session, String query, LogicalPlan plan,
                              SubmitQueryResponse.Builder response) throws Exception {
    ScanNode scanNode = plan.getRootBlock().getNode(NodeType.SCAN);
    if (scanNode == null) {
      scanNode = plan.getRootBlock().getNode(NodeType.PARTITIONS_SCAN);
    }
    TableDesc desc = scanNode.getTableDesc();
    // Keep info for partition-column-only queries
    SelectionNode selectionNode = plan.getRootBlock().getNode(NodeType.SELECTION);
    if (desc.isExternal() && desc.hasPartition() && selectionNode != null) {
      scanNode.setQual(selectionNode.getQual());
    }
    int maxRow = Integer.MAX_VALUE;
    if (plan.getRootBlock().hasNode(NodeType.LIMIT)) {
      LimitNode limitNode = plan.getRootBlock().getNode(NodeType.LIMIT);
      maxRow = (int) limitNode.getFetchFirstNum();
    }
    if (desc.getStats().getNumRows() == 0) {
      desc.getStats().setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
    }

    QueryInfo queryInfo = context.getQueryJobManager().createNewSimpleQuery(queryContext, session, query,
        (LogicalRootNode) plan.getRootBlock().getRoot());

    NonForwardQueryResultScanner queryResultScanner = new NonForwardQueryResultFileScanner(
        context.getConf(), session.getSessionId(), queryInfo.getQueryId(), scanNode, desc, maxRow);

    queryResultScanner.init();
    session.addNonForwardQueryResultScanner(queryResultScanner);

    response.setQueryId(queryInfo.getQueryId().getProto());
    response.setMaxRowNum(maxRow);
    response.setTableDesc(desc.getProto());
    response.setResultCode(ClientProtos.ResultCode.OK);
  }

  public void execNonFromQuery(QueryContext queryContext, LogicalPlan plan, SubmitQueryResponse.Builder responseBuilder)
      throws Exception {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    EvalContext evalContext = new EvalContext();
    Target[] targets = plan.getRootBlock().getRawTargets();
    if (targets == null) {
      throw new PlanningException("No targets");
    }
    try {
      // start script executor
      startScriptExecutors(queryContext, evalContext, targets);
      final Tuple outTuple = new VTuple(targets.length);
      for (int i = 0; i < targets.length; i++) {
        EvalNode eval = targets[i].getEvalTree();
        eval.bind(evalContext, null);
        outTuple.put(i, eval.eval(null));
      }
      boolean isInsert = rootNode.getChild() != null && rootNode.getChild().getType() == NodeType.INSERT;
      if (isInsert) {
        InsertNode insertNode = rootNode.getChild();
        insertNonFromQuery(queryContext, insertNode, responseBuilder);
      } else {
        Schema schema = PlannerUtil.targetToSchema(targets);
        RowStoreUtil.RowStoreEncoder encoder = RowStoreUtil.createEncoder(schema);
        byte[] serializedBytes = encoder.toBytes(outTuple);
        ClientProtos.SerializedResultSet.Builder serializedResBuilder = ClientProtos.SerializedResultSet.newBuilder();
        serializedResBuilder.addSerializedTuples(ByteString.copyFrom(serializedBytes));
        serializedResBuilder.setSchema(schema.getProto());
        serializedResBuilder.setBytesNum(serializedBytes.length);

        responseBuilder.setResultSet(serializedResBuilder);
        responseBuilder.setMaxRowNum(1);
        responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
        responseBuilder.setResultCode(ClientProtos.ResultCode.OK);
      }
    } finally {
      // stop script executor
      stopScriptExecutors(evalContext);
    }
  }

  public static void startScriptExecutors(QueryContext queryContext, EvalContext evalContext, Target[] targets)
      throws IOException {
    for (int i = 0; i < targets.length; i++) {
      EvalNode eval = targets[i].getEvalTree();
      if (eval instanceof GeneralFunctionEval) {
        GeneralFunctionEval functionEval = (GeneralFunctionEval) eval;
        if (functionEval.getFuncDesc().getInvocation().hasPython()) {
          TajoScriptEngine scriptExecutor = new PythonScriptEngine(functionEval.getFuncDesc());
          evalContext.addScriptEngine(eval, scriptExecutor);
          scriptExecutor.start(queryContext.getConf());
        }
      }
    }
  }

  public static void stopScriptExecutors(EvalContext evalContext) {
    for (TajoScriptEngine executor : evalContext.getAllScriptEngines()) {
      executor.shutdown();
    }
  }

  private void insertNonFromQuery(QueryContext queryContext,
                                  InsertNode insertNode, SubmitQueryResponse.Builder responseBuilder)
      throws Exception {
    String nodeUniqName = insertNode.getTableName() == null ? insertNode.getPath().getName() : insertNode.getTableName();
    String queryId = nodeUniqName + "_" + System.currentTimeMillis();

    FileSystem fs = TajoConf.getWarehouseDir(context.getConf()).getFileSystem(context.getConf());
    Path stagingDir = QueryMasterTask.initStagingDir(context.getConf(), queryId.toString(), queryContext);
    Path stagingResultDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);

    TableDesc tableDesc = null;
    Path finalOutputDir = null;
    if (insertNode.getTableName() != null) {
      tableDesc = this.catalog.getTableDesc(insertNode.getTableName());
      finalOutputDir = new Path(tableDesc.getPath());
    } else {
      finalOutputDir = insertNode.getPath();
    }

    TaskAttemptContext taskAttemptContext = new TaskAttemptContext(queryContext, null, null, null, stagingDir);
    taskAttemptContext.setOutputPath(new Path(stagingResultDir, "part-01-000000"));

    EvalExprExec evalExprExec = new EvalExprExec(taskAttemptContext, (EvalExprNode) insertNode.getChild());
    StoreTableExec exec = new StoreTableExec(taskAttemptContext, insertNode, evalExprExec);
    try {
      exec.init();
      exec.next();
    } finally {
      exec.close();
    }

    if (insertNode.isOverwrite()) { // INSERT OVERWRITE INTO
      // it moves the original table into the temporary location.
      // Then it moves the new result table into the original table location.
      // Upon failed, it recovers the original table if possible.
      boolean movedToOldTable = false;
      boolean committed = false;
      Path oldTableDir = new Path(stagingDir, TajoConstants.INSERT_OVERWIRTE_OLD_TABLE_NAME);
      try {
        if (fs.exists(finalOutputDir)) {
          fs.rename(finalOutputDir, oldTableDir);
          movedToOldTable = fs.exists(oldTableDir);
        } else { // if the parent does not exist, make its parent directory.
          fs.mkdirs(finalOutputDir.getParent());
        }
        fs.rename(stagingResultDir, finalOutputDir);
        committed = fs.exists(finalOutputDir);
      } catch (IOException ioe) {
        // recover the old table
        if (movedToOldTable && !committed) {
          fs.rename(oldTableDir, finalOutputDir);
        }
      }
    } else {
      FileStatus[] files = fs.listStatus(stagingResultDir);
      for (FileStatus eachFile: files) {
        Path targetFilePath = new Path(finalOutputDir, eachFile.getPath().getName());
        if (fs.exists(targetFilePath)) {
          targetFilePath = new Path(finalOutputDir, eachFile.getPath().getName() + "_" + System.currentTimeMillis());
        }
        fs.rename(eachFile.getPath(), targetFilePath);
      }
    }

    if (insertNode.hasTargetTable()) {
      TableStats stats = tableDesc.getStats();
      long volume = Query.getTableVolume(context.getConf(), finalOutputDir);
      stats.setNumBytes(volume);
      stats.setNumRows(1);

      CatalogProtos.UpdateTableStatsProto.Builder builder = CatalogProtos.UpdateTableStatsProto.newBuilder();
      builder.setTableName(tableDesc.getName());
      builder.setStats(stats.getProto());

      catalog.updateTableStats(builder.build());

      responseBuilder.setTableDesc(tableDesc.getProto());
    } else {
      TableStats stats = new TableStats();
      long volume = Query.getTableVolume(context.getConf(), finalOutputDir);
      stats.setNumBytes(volume);
      stats.setNumRows(1);

      // Empty TableDesc
      List<CatalogProtos.ColumnProto> columns = new ArrayList<CatalogProtos.ColumnProto>();
      CatalogProtos.TableDescProto tableDescProto = CatalogProtos.TableDescProto.newBuilder()
          .setTableName(nodeUniqName)
          .setMeta(CatalogProtos.TableProto.newBuilder().setStoreType(CatalogProtos.StoreType.CSV).build())
          .setSchema(CatalogProtos.SchemaProto.newBuilder().addAllFields(columns).build())
          .setStats(stats.getProto())
          .build();

      responseBuilder.setTableDesc(tableDescProto);
    }

    // If queryId == NULL_QUERY_ID and MaxRowNum == -1, TajoCli prints only number of inserted rows.
    responseBuilder.setMaxRowNum(-1);
    responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
    responseBuilder.setResultCode(ClientProtos.ResultCode.OK);
  }

  public void executeDistributedQuery(QueryContext queryContext, Session session,
                                      LogicalPlan plan,
                                      String sql,
                                      String jsonExpr,
                                      SubmitQueryResponse.Builder responseBuilder) throws Exception {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    CatalogProtos.StoreType storeType = PlannerUtil.getStoreType(plan);
    if (storeType != null) {
      StorageManager sm = StorageManager.getStorageManager(context.getConf(), storeType);
      StorageProperty storageProperty = sm.getStorageProperty();
      if (!storageProperty.isSupportsInsertInto()) {
        throw new VerifyException("Inserting into non-file storage is not supported.");
      }
      sm.beforeInsertOrCATS(rootNode.getChild());
    }
    context.getSystemMetrics().counter("Query", "numDMLQuery").inc();
    hookManager.doHooks(queryContext, plan);

    QueryManager queryManager = this.context.getQueryJobManager();
    QueryInfo queryInfo;

    queryInfo = queryManager.scheduleQuery(session, queryContext, sql, jsonExpr, rootNode);

    if(queryInfo == null) {
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setResultCode(ClientProtos.ResultCode.ERROR);
      responseBuilder.setErrorMessage("Fail starting QueryMaster.");
      LOG.error("Fail starting QueryMaster: " + sql);
    } else {
      responseBuilder.setIsForwarded(true);
      responseBuilder.setQueryId(queryInfo.getQueryId().getProto());
      responseBuilder.setResultCode(ClientProtos.ResultCode.OK);
      if(queryInfo.getQueryMasterHost() != null) {
        responseBuilder.setQueryMasterHost(queryInfo.getQueryMasterHost());
      }
      responseBuilder.setQueryMasterPort(queryInfo.getQueryMasterClientPort());
      LOG.info("Query " + queryInfo.getQueryId().toString() + "," + queryInfo.getSql() + "," +
          " is forwarded to " + queryInfo.getQueryMasterHost() + ":" + queryInfo.getQueryMasterPort());
    }
  }

  public static MasterPlan compileMasterPlan(LogicalPlan plan, QueryContext context, GlobalPlanner planner)
      throws Exception {

    CatalogProtos.StoreType storeType = PlannerUtil.getStoreType(plan);
    if (storeType != null) {
      StorageManager sm = StorageManager.getStorageManager(planner.getConf(), storeType);
      StorageProperty storageProperty = sm.getStorageProperty();
      if (storageProperty.isSortedInsert()) {
        String tableName = PlannerUtil.getStoreTableName(plan);
        LogicalRootNode rootNode = plan.getRootBlock().getRoot();
        TableDesc tableDesc = PlannerUtil.getTableDesc(planner.getCatalog(), rootNode.getChild());
        if (tableDesc == null) {
          throw new VerifyException("Can't get table meta data from catalog: " + tableName);
        }
        List<LogicalPlanRewriteRule> storageSpecifiedRewriteRules = sm.getRewriteRules(
            context, tableDesc);
        if (storageSpecifiedRewriteRules != null) {
          for (LogicalPlanRewriteRule eachRule: storageSpecifiedRewriteRules) {
            eachRule.rewrite(context, plan);
          }
        }
      }
    }

    MasterPlan masterPlan = new MasterPlan(QueryIdFactory.NULL_QUERY_ID, context, plan);
    planner.build(masterPlan);

    return masterPlan;
  }
}
