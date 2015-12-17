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
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.physical.EvalExprExec;
import org.apache.tajo.engine.planner.physical.InsertRowsExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.DuplicateIndexException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.ipc.ClientProtos.SerializedResultSet;
import org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;
import org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse.ResultType;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.master.QueryManager;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.exec.prehook.CreateTableHook;
import org.apache.tajo.master.exec.prehook.DistributedQueryHookManager;
import org.apache.tajo.master.exec.prehook.InsertIntoHook;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalContext;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.GeneralFunctionEval;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.plan.function.python.TajoScriptEngine;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.session.Session;
import org.apache.tajo.storage.*;
import org.apache.tajo.tuple.memory.MemoryBlock;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.util.ProtoUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.tajo.exception.ReturnStateUtil.OK;
import static org.apache.tajo.exception.ReturnStateUtil.errUndefinedDatabase;

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
    response.setUserName(queryContext.get(SessionVars.USERNAME));

    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    if (PlannerUtil.checkIfSetSession(rootNode)) {
      execSetSession(session, plan, response);


    } else if (PlannerUtil.checkIfDDLPlan(rootNode)) {

      if (PlannerUtil.isDistExecDDL(rootNode)) {
        if (rootNode.getChild().getType() == NodeType.CREATE_INDEX) {
          checkIndexExistence(queryContext, (CreateIndexNode) rootNode.getChild());
        }
        executeDistributedQuery(queryContext, session, plan, sql, jsonExpr, response);
      } else {
        ddlExecutor.execute(queryContext, plan);
        response.setState(OK);
        response.setResultType(ResultType.NO_RESULT);
      }

    } else if (plan.isExplain()) { // explain query
      execExplain(session, sql, plan, queryContext, plan.isExplainGlobal(), response);

    } else if (PlannerUtil.checkIfQueryTargetIsVirtualTable(plan)) {
      execQueryOnVirtualTable(queryContext, session, sql, plan, response);

      // Simple query indicates a form of 'select * from tb_name [LIMIT X];'.
    } else if (PlannerUtil.checkIfSimpleQuery(plan)) {
      execSimpleQuery(queryContext, session, sql, plan, response);

      // NonFromQuery indicates a form of 'select a, x+y;'
    } else if (PlannerUtil.checkIfNonFromQuery(plan)) {
      execNonFromQuery(queryContext, session, sql, plan, response);

    } else { // it requires distributed execution. So, the query is forwarded to a query master.

      // Checking if CTAS is already finished due to 'IF NOT EXISTS' option
      if (checkIfCtasAlreadyDone(rootNode)) {
        response.setState(OK);
        response.setResultType(ResultType.NO_RESULT);
      } else {
        executeDistributedQuery(queryContext, session, plan, sql, jsonExpr, response);
      }
    }

    response.setSessionVars(ProtoUtil.convertFromMap(session.getAllVariables()));

    return response.build();
  }

  /**
   * Check if CTAS is already done
   * @param rootNode
   * @return
   */
  private boolean checkIfCtasAlreadyDone(LogicalNode rootNode) {
    if (rootNode.getChild(0).getType() == NodeType.CREATE_TABLE) {
      CreateTableNode createTable = (CreateTableNode) rootNode.getChild(0);
      if (createTable.isIfNotExists() && catalog.existsTable(createTable.getTableName())) {
        return true;
      }
    }

    return false;
  }

  public void execSetSession(Session session, LogicalPlan plan,
                             SubmitQueryResponse.Builder response) {
    SetSessionNode setSessionNode = ((LogicalRootNode) plan.getRootBlock().getRoot()).getChild();

    final String varName = setSessionNode.getName();

    // SET CATALOG 'XXX'
    if (varName.equals(SessionVars.CURRENT_DATABASE.name())) {
      String databaseName = setSessionNode.getValue();

      if (catalog.existDatabase(databaseName)) {
        session.selectDatabase(setSessionNode.getValue());
      } else {
        response.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
        response.setState(errUndefinedDatabase(databaseName));
      }

      // others
    } else {
      if (setSessionNode.hasValue()) {
        session.setVariable(varName, setSessionNode.getValue());
      } else {
        session.removeVariable(varName);
      }
    }

    response.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
    response.setState(OK);
  }

  public void execExplain(Session session, String query, LogicalPlan plan, QueryContext queryContext, boolean isGlobal,
                          SubmitQueryResponse.Builder response) throws Exception {

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

    SerializedResultSet.Builder serializedResBuilder = SerializedResultSet.newBuilder();
    MemoryRowBlock rowBlock = new MemoryRowBlock(SchemaUtil.toDataTypes(schema));
    String[] lines = explainStr.split("\n");
    try {
      for (String line : lines) {
        rowBlock.getWriter().startRow();
        rowBlock.getWriter().putText(line);
        rowBlock.getWriter().endRow();
      }
      MemoryBlock memoryBlock = rowBlock.getMemory();
      ByteBuffer uncompressed = memoryBlock.getBuffer().nioBuffer(0, memoryBlock.readableBytes());
      int uncompressedLength = uncompressed.remaining();

      serializedResBuilder.setDecompressedLength(uncompressedLength);
      serializedResBuilder.setSerializedTuples(ByteString.copyFrom(uncompressed));
      serializedResBuilder.setSchema(schema.getProto());
      serializedResBuilder.setRows(rowBlock.rows());
    } finally {
      rowBlock.release();
    }

    QueryInfo queryInfo = context.getQueryJobManager().createNewSimpleQuery(queryContext, session, query,
        (LogicalRootNode) plan.getRootBlock().getRoot());

    response.setState(OK);
    response.setQueryId(queryInfo.getQueryId().getProto());
    response.setResultType(ResultType.ENCLOSED);
    response.setResultSet(serializedResBuilder.build());
    response.setMaxRowNum(lines.length);
  }

  public void execQueryOnVirtualTable(QueryContext queryContext, Session session, String query, LogicalPlan plan,
                              SubmitQueryResponse.Builder response) throws Exception {
    int maxRow = Integer.MAX_VALUE;
    if (plan.getRootBlock().hasNode(NodeType.LIMIT)) {
      LimitNode limitNode = plan.getRootBlock().getNode(NodeType.LIMIT);
      maxRow = (int) limitNode.getFetchFirstNum();
    }
    QueryInfo queryInfo = context.getQueryJobManager().createNewSimpleQuery(queryContext, session, query,
        (LogicalRootNode) plan.getRootBlock().getRoot());

    NonForwardQueryResultScanner queryResultScanner = new NonForwardQueryResultSystemScanner(
        context,
        plan,
        queryInfo.getQueryId(),
        session.getSessionId(),
        maxRow);

    queryResultScanner.init();
    session.addNonForwardQueryResultScanner(queryResultScanner);

    response.setState(OK);
    response.setQueryId(queryInfo.getQueryId().getProto());
    response.setResultType(ResultType.ENCLOSED);
    response.setMaxRowNum(maxRow);
    response.setTableDesc(queryResultScanner.getTableDesc().getProto());
  }

  public void execSimpleQuery(QueryContext queryContext, Session session, String query, LogicalPlan plan,
                              SubmitQueryResponse.Builder response) throws Exception {

    ScanNode scanNode = plan.getRootBlock().getNode(NodeType.SCAN);
    final TableDesc table = scanNode.getTableDesc();
    if (table.hasPartition()) {
      scanNode = plan.getRootBlock().getNode(NodeType.PARTITIONS_SCAN);
    }

    final TableDesc resultDesc = new TableDesc("", scanNode.getOutSchema(),
        new TableMeta(BuiltinStorages.DRAW, table.getMeta().getPropertySet()), null);

    // push down limit
    int maxRow = Integer.MAX_VALUE;
    if (plan.getRootBlock().hasNode(NodeType.LIMIT)) {
      LimitNode limitNode = plan.getRootBlock().getNode(NodeType.LIMIT);
      maxRow = (int) limitNode.getFetchFirstNum();
      scanNode.setLimit(maxRow);
    }

    final QueryInfo queryInfo = context.getQueryJobManager().createNewSimpleQuery(queryContext, session, query,
        plan.getRootBlock().getRoot());

    final NonForwardQueryResultScanner queryResultScanner = new NonForwardQueryResultFileScanner(
        context.asyncTaskExecutor(),
        context.getConf(),
        session.getSessionId(),
        queryInfo.getQueryId(),
        scanNode,
        maxRow,
        Optional.empty());

    queryResultScanner.init();

    session.addNonForwardQueryResultScanner(queryResultScanner);

    response.setState(OK);
    response.setQueryId(queryInfo.getQueryId().getProto());
    response.setResultType(ResultType.ENCLOSED);
    response.setMaxRowNum(maxRow);
    response.setTableDesc(resultDesc.getProto());
  }

  public void execNonFromQuery(QueryContext queryContext, Session session, String query, LogicalPlan plan, SubmitQueryResponse.Builder responseBuilder)
      throws Exception {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    EvalContext evalContext = new EvalContext();
    List<Target> targets = plan.getRootBlock().getRawTargets();
    if (targets == null) {
      throw new TajoInternalError("no targets");
    }
    try {
      // start script executor
      startScriptExecutors(queryContext, evalContext, targets);
      final VTuple outTuple = new VTuple(targets.size());
      for (int i = 0; i < targets.size(); i++) {
        EvalNode eval = targets.get(i).getEvalTree();
        eval.bind(evalContext, null);
        outTuple.put(i, eval.eval(null));
      }
      boolean isInsert = rootNode.getChild() != null && rootNode.getChild().getType() == NodeType.INSERT;
      if (isInsert) {
        InsertNode insertNode = rootNode.getChild();
        insertRowValues(queryContext, insertNode, responseBuilder);
      } else {
        Schema schema = PlannerUtil.targetToSchema(targets);
        SerializedResultSet.Builder serializedResBuilder = SerializedResultSet.newBuilder();
        MemoryRowBlock rowBlock = new MemoryRowBlock(SchemaUtil.toDataTypes(schema));

        try {
          rowBlock.getWriter().addTuple(outTuple);

          MemoryBlock memoryBlock = rowBlock.getMemory();
          ByteBuffer uncompressed = memoryBlock.getBuffer().nioBuffer(0, memoryBlock.readableBytes());
          int uncompressedLength = uncompressed.remaining();

          serializedResBuilder.setDecompressedLength(uncompressedLength);
          serializedResBuilder.setSerializedTuples(ByteString.copyFrom(uncompressed));
          serializedResBuilder.setSchema(schema.getProto());
          serializedResBuilder.setRows(rowBlock.rows());
        } finally {
          rowBlock.release();
        }

        QueryInfo queryInfo = context.getQueryJobManager().createNewSimpleQuery(queryContext, session, query,
            (LogicalRootNode) plan.getRootBlock().getRoot());

        responseBuilder.setState(OK);
        responseBuilder.setResultType(ResultType.ENCLOSED);
        responseBuilder.setQueryId(queryInfo.getQueryId().getProto());
        responseBuilder.setResultSet(serializedResBuilder);
        responseBuilder.setMaxRowNum(1);
      }
    } finally {
      // stop script executor
      stopScriptExecutors(evalContext);
    }
  }

  public static void startScriptExecutors(QueryContext queryContext, EvalContext evalContext, List<Target> targets)
      throws IOException {
    for (int i = 0; i < targets.size(); i++) {
      EvalNode eval = targets.get(i).getEvalTree();
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

  /**
   * Insert rows through staging phase
   */
  private void insertRowsThroughStaging(TaskAttemptContext taskAttemptContext,
                                        InsertNode insertNode,
                                        Path finalOutputPath,
                                        Path stagingDir,
                                        Path stagingResultDir)
      throws IOException {

    EvalExprExec evalExprExec = new EvalExprExec(taskAttemptContext, (EvalExprNode) insertNode.getChild());
    InsertRowsExec exec = new InsertRowsExec(taskAttemptContext, insertNode, evalExprExec);

    try {
      exec.init();
      exec.next();
    } finally {
      exec.close();
    }

    FileSystem fs = TajoConf.getWarehouseDir(context.getConf()).getFileSystem(context.getConf());

    if (insertNode.isOverwrite()) { // INSERT OVERWRITE INTO
      // it moves the original table into the temporary location.
      // Then it moves the new result table into the original table location.
      // Upon failed, it recovers the original table if possible.
      boolean movedToOldTable = false;
      boolean committed = false;
      Path oldTableDir = new Path(stagingDir, TajoConstants.INSERT_OVERWIRTE_OLD_TABLE_NAME);
      try {
        if (fs.exists(finalOutputPath)) {
          fs.rename(finalOutputPath, oldTableDir);
          movedToOldTable = fs.exists(oldTableDir);
        } else { // if the parent does not exist, make its parent directory.
          fs.mkdirs(finalOutputPath.getParent());
        }
        fs.rename(stagingResultDir, finalOutputPath);
        committed = fs.exists(finalOutputPath);
      } catch (IOException ioe) {
        // recover the old table
        if (movedToOldTable && !committed) {
          fs.rename(oldTableDir, finalOutputPath);
        }
      }
    } else {
      FileStatus[] files = fs.listStatus(stagingResultDir);
      for (FileStatus eachFile : files) {
        Path targetFilePath = new Path(finalOutputPath, eachFile.getPath().getName());
        if (fs.exists(targetFilePath)) {
          targetFilePath = new Path(finalOutputPath, eachFile.getPath().getName() + "_" + System.currentTimeMillis());
        }
        fs.rename(eachFile.getPath(), targetFilePath);
      }
    }
  }

  /**
   * Insert row values
   */
  private void insertRowValues(QueryContext queryContext,
                               InsertNode insertNode, SubmitQueryResponse.Builder responseBuilder) {
    try {
      String nodeUniqName = insertNode.getTableName() == null ? new Path(insertNode.getUri()).getName() :
          insertNode.getTableName();
      String queryId = nodeUniqName + "_" + System.currentTimeMillis();

      URI finalOutputUri = insertNode.getUri();
      Tablespace space = TablespaceManager.get(finalOutputUri);
      TableMeta tableMeta = new TableMeta(insertNode.getStorageType(), insertNode.getOptions());
      tableMeta.putProperty(StorageConstants.INSERT_DIRECTLY, Boolean.TRUE.toString());

      FormatProperty formatProperty = space.getFormatProperty(tableMeta);

      TaskAttemptContext taskAttemptContext;
      if (formatProperty.directInsertSupported()) { // if this format and storage supports direct insertion
        taskAttemptContext = new TaskAttemptContext(queryContext, null, null, null, null);
        taskAttemptContext.setOutputPath(new Path(finalOutputUri));

        EvalExprExec evalExprExec = new EvalExprExec(taskAttemptContext, insertNode.getChild());
        InsertRowsExec exec = new InsertRowsExec(taskAttemptContext, insertNode, evalExprExec);

        try {
          exec.init();
          exec.next();
        } finally {
          exec.close();
        }
      } else {
        URI stagingSpaceUri = space.prepareStagingSpace(context.getConf(), queryId, queryContext, tableMeta);
        Path stagingDir = new Path(stagingSpaceUri);
        Path stagingResultDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);

        taskAttemptContext = new TaskAttemptContext(queryContext, null, null, null, stagingDir);
        taskAttemptContext.setOutputPath(new Path(stagingResultDir, "part-01-000000"));
        insertRowsThroughStaging(taskAttemptContext, insertNode, new Path(finalOutputUri), stagingDir, stagingResultDir);
      }

      // set insert stats (how many rows and bytes)
      TableStats stats = new TableStats();
      stats.setNumBytes(taskAttemptContext.getResultStats().getNumBytes());
      stats.setNumRows(taskAttemptContext.getResultStats().getNumRows());

      if (insertNode.hasTargetTable()) {
        CatalogProtos.UpdateTableStatsProto.Builder builder = CatalogProtos.UpdateTableStatsProto.newBuilder();
        builder.setTableName(insertNode.getTableName());
        builder.setStats(stats.getProto());

        catalog.updateTableStats(builder.build());

        TableDesc desc = new TableDesc(
            insertNode.getTableName(),
            insertNode.getTargetSchema(),
            tableMeta,
            finalOutputUri);
        responseBuilder.setTableDesc(desc.getProto());

      } else { // If INSERT INTO LOCATION

        // Empty TableDesc
        List<CatalogProtos.ColumnProto> columns = new ArrayList<>();
        CatalogProtos.TableDescProto tableDescProto = CatalogProtos.TableDescProto.newBuilder()
            .setTableName(nodeUniqName)
            .setMeta(CatalogProtos.TableProto.newBuilder().setDataFormat(BuiltinStorages.TEXT).build())
            .setSchema(CatalogProtos.SchemaProto.newBuilder().addAllFields(columns).build())
            .setStats(stats.getProto())
            .build();

        responseBuilder.setTableDesc(tableDescProto);
      }

      // If queryId == NULL_QUERY_ID and MaxRowNum == -1, TajoCli prints only number of inserted rows.
      responseBuilder.setMaxRowNum(-1);
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setResultType(ResultType.NO_RESULT);
      responseBuilder.setState(OK);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public void executeDistributedQuery(QueryContext queryContext, Session session,
                                      LogicalPlan plan,
                                      String sql,
                                      String jsonExpr,
                                      SubmitQueryResponse.Builder responseBuilder) throws Exception {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    prepareForCreateTableOrInsert(catalog, plan);

    hookManager.doHooks(queryContext, plan);

    QueryManager queryManager = this.context.getQueryJobManager();
    QueryInfo queryInfo;

    queryInfo = queryManager.scheduleQuery(session, queryContext, sql, jsonExpr, rootNode);

    responseBuilder.setState(OK);
    responseBuilder.setQueryId(queryInfo.getQueryId().getProto());
    responseBuilder.setResultType(ResultType.FETCH);
    if (queryInfo.getQueryMasterHost() != null) {
      responseBuilder.setQueryMasterHost(queryInfo.getQueryMasterHost());
    }
    responseBuilder.setQueryMasterPort(queryInfo.getQueryMasterClientPort());
    LOG.info("Query " + queryInfo.getQueryId().toString() + "," + queryInfo.getSql() + "," +
        " is forwarded to " + queryInfo.getQueryMasterHost() + ":" + queryInfo.getQueryMasterPort());
  }

  private void prepareForCreateTableOrInsert(CatalogService catalog, LogicalPlan plan)
      throws TajoException, IOException {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
    TableDesc tableDesc = PlannerUtil.getTableDesc(catalog, plan.getRootBlock().getRoot());
    if (tableDesc != null) {

      Tablespace space = TablespaceManager.get(tableDesc.getUri());
      FormatProperty formatProperty = space.getFormatProperty(tableDesc.getMeta());

      if (!formatProperty.isInsertable()) {
        throw new UnsupportedException (
            String.format("INSERT operation on %s tablespace", tableDesc.getUri().toString()));
      }

      space.prepareTable(rootNode.getChild());
    }
  }

  private void checkIndexExistence(final QueryContext queryContext, final CreateIndexNode createIndexNode)
      throws DuplicateIndexException {

    String databaseName, simpleIndexName, qualifiedIndexName;
    if (CatalogUtil.isFQTableName(createIndexNode.getIndexName())) {
      String[] splits = CatalogUtil.splitFQTableName(createIndexNode.getIndexName());
      databaseName = splits[0];
      simpleIndexName = splits[1];
      qualifiedIndexName = createIndexNode.getIndexName();
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleIndexName = createIndexNode.getIndexName();
      qualifiedIndexName = CatalogUtil.buildFQName(databaseName, simpleIndexName);
    }

    if (catalog.existIndexByName(databaseName, simpleIndexName)) {
      throw new DuplicateIndexException(qualifiedIndexName);
    }
  }

  public MasterPlan compileMasterPlan(LogicalPlan plan, QueryContext context, GlobalPlanner planner)
      throws Exception {

    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
    TableDesc tableDesc = PlannerUtil.getTableDesc(planner.getCatalog(), rootNode.getChild());

    if (tableDesc != null) {
      Tablespace space = TablespaceManager.get(tableDesc.getUri());
      space.rewritePlan(context, plan);
    }

    MasterPlan masterPlan = new MasterPlan(QueryIdFactory.NULL_QUERY_ID, context, plan);
    planner.build(context, masterPlan);

    return masterPlan;
  }
}
