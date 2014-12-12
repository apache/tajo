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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.algebra.AlterTablespaceSetType;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.JsonHelper;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.physical.EvalExprExec;
import org.apache.tajo.engine.planner.physical.StoreTableExec;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.RequestResult;
import org.apache.tajo.ipc.ClientProtos.ResultCode;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.querymaster.Query;
import org.apache.tajo.master.querymaster.QueryInfo;
import org.apache.tajo.master.querymaster.QueryJobManager;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.plan.*;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.verifier.LogicalPlanVerifier;
import org.apache.tajo.plan.verifier.PreLogicalPlanVerifier;
import org.apache.tajo.plan.verifier.VerificationState;
import org.apache.tajo.plan.verifier.VerifyException;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.ProtoUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto;
import static org.apache.tajo.catalog.proto.CatalogProtos.UpdateTableStatsProto;
import static org.apache.tajo.ipc.ClientProtos.SerializedResultSet;
import static org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;

public class GlobalEngine extends AbstractService {
  /** Class Logger */
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final MasterContext context;
  private final StorageManager sm;

  private SQLAnalyzer analyzer;
  private CatalogService catalog;
  private PreLogicalPlanVerifier preVerifier;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private LogicalPlanVerifier annotatedPlanVerifier;
  private DistributedQueryHookManager hookManager;

  public GlobalEngine(final MasterContext context) {
    super(GlobalEngine.class.getName());
    this.context = context;
    this.catalog = context.getCatalog();
    this.sm = context.getStorageManager();
  }

  public void start() {
    try  {
      analyzer = new SQLAnalyzer();
      preVerifier = new PreLogicalPlanVerifier(context.getCatalog());
      planner = new LogicalPlanner(context.getCatalog());
      // Access path rewriter is enabled only in QueryMasterTask
      optimizer = new LogicalOptimizer(context.getConf(), context.getCatalog());
      annotatedPlanVerifier = new LogicalPlanVerifier(context.getConf(), context.getCatalog());

      hookManager = new DistributedQueryHookManager();
      hookManager.addHook(new CreateTableHook());
      hookManager.addHook(new InsertHook());
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    super.start();
  }

  public void stop() {
    super.stop();
  }

  @VisibleForTesting
  public SQLAnalyzer getAnalyzer() {
    return analyzer;
  }

  @VisibleForTesting
  public PreLogicalPlanVerifier getPreLogicalPlanVerifier() {
    return preVerifier;
  }

  @VisibleForTesting
  public LogicalPlanner getLogicalPlanner() {
    return planner;
  }

  @VisibleForTesting
  public LogicalOptimizer getLogicalOptimizer() {
    return optimizer;
  }

  public LogicalPlanVerifier getLogicalPlanVerifier() {
    return annotatedPlanVerifier;
  }

  private QueryContext createQueryContext(Session session) {
    QueryContext newQueryContext =  new QueryContext(context.getConf(), session);

    String tajoTest = System.getProperty(CommonTestingUtil.TAJO_TEST_KEY);
    if (tajoTest != null && tajoTest.equalsIgnoreCase(CommonTestingUtil.TAJO_TEST_TRUE)) {
      newQueryContext.putAll(CommonTestingUtil.getSessionVarsForTest());
    }

    return newQueryContext;
  }

  public SubmitQueryResponse executeQuery(Session session, String query, boolean isJson) {
    LOG.info("Query: " + query);
    QueryContext queryContext = createQueryContext(session);
    Expr planningContext;

    try {
      if (isJson) {
        planningContext = buildExpressionFromJson(query);
      } else {
        planningContext = buildExpressionFromSql(queryContext, query);
      }

      String jsonExpr = planningContext.toJson();
      LogicalPlan plan = createLogicalPlan(queryContext, planningContext);
      SubmitQueryResponse response = executeQueryInternal(queryContext, session, plan, query, jsonExpr);
      return response;
    } catch (Throwable t) {
      context.getSystemMetrics().counter("Query", "errorQuery").inc();
      LOG.error("\nStack Trace:\n" + StringUtils.stringifyException(t));
      SubmitQueryResponse.Builder responseBuilder = SubmitQueryResponse.newBuilder();
      responseBuilder.setUserName(queryContext.get(SessionVars.USERNAME));
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setIsForwarded(true);
      String errorMessage = t.getMessage();
      if (t.getMessage() == null) {
        errorMessage = t.getClass().getName();
      }
      responseBuilder.setResult(buildRequestResult(ResultCode.ERROR,
          errorMessage, StringUtils.stringifyException(t)));
      return responseBuilder.build();
    }
  }

  public Expr buildExpressionFromJson(String json) {
    return JsonHelper.fromJson(json, Expr.class);
  }

  public Expr buildExpressionFromSql(QueryContext queryContext, String sql)
      throws InterruptedException, IOException, IllegalQueryStatusException {
    context.getSystemMetrics().counter("Query", "totalQuery").inc();
    return analyzer.parse(sql);
  }

  private SubmitQueryResponse executeQueryInternal(QueryContext queryContext,
                                                      Session session,
                                                      LogicalPlan plan,
                                                      String sql,
                                                      String jsonExpr) throws Exception {

    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    SubmitQueryResponse.Builder responseBuilder = SubmitQueryResponse.newBuilder();
    SubmitQueryResponse response = null;
    responseBuilder.setIsForwarded(false);
    responseBuilder.setUserName(queryContext.get(SessionVars.USERNAME));

    if (PlannerUtil.checkIfSetSession(rootNode)) {

      SetSessionNode setSessionNode = rootNode.getChild();

      final String varName = setSessionNode.getName();

      // SET CATALOG 'XXX'
      if (varName.equals(SessionVars.CURRENT_DATABASE.name())) {
        String databaseName = setSessionNode.getValue();

        if (catalog.existDatabase(databaseName)) {
          session.selectDatabase(setSessionNode.getValue());
        } else {
          responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
          responseBuilder.setResult(buildRequestResult(ResultCode.ERROR, "database \"" + databaseName + "\" does not exists.", null));
          responseBuilder.setSessionVars(ProtoUtil.convertFromMap(session.getAllVariables()));
          return responseBuilder.build();
        }

        // others
      } else {
        if (setSessionNode.isDefaultValue()) {
          session.removeVariable(varName);
        } else {
          session.setVariable(varName, setSessionNode.getValue());
        }
      }

      context.getSystemMetrics().counter("Query", "numDDLQuery").inc();
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setResult(buildOkRequestResult());

    } else if (PlannerUtil.checkIfDDLPlan(rootNode)) {
      context.getSystemMetrics().counter("Query", "numDDLQuery").inc();
      if (PlannerUtil.requireParallelExecution(rootNode)) {
        if (rootNode.getChild().getType() == NodeType.CREATE_INDEX) {
          checkIndexExistence(queryContext, (CreateIndexNode) rootNode.getChild());
        }
        response = submitForwardQuery(session, queryContext, sql, jsonExpr, rootNode, responseBuilder);
      } else {
        responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
        responseBuilder.setResult(buildOkRequestResult());
      }
      updateQuery(queryContext, rootNode.getChild());

    } else if (plan.isExplain()) { // explain query
      String explainStr = PlannerUtil.buildExplainString(plan.getRootBlock().getRoot());
      Schema schema = new Schema();
      schema.addColumn("explain", TajoDataTypes.Type.TEXT);
      RowStoreUtil.RowStoreEncoder encoder = RowStoreUtil.createEncoder(schema);

      SerializedResultSet.Builder serializedResBuilder = SerializedResultSet.newBuilder();

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

      responseBuilder.setResultSet(serializedResBuilder.build());
      responseBuilder.setMaxRowNum(lines.length);
      responseBuilder.setResult(buildOkRequestResult());
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());

      // Simple query indicates a form of 'select * from tb_name [LIMIT X];'.
    } else if (PlannerUtil.checkIfSimpleQuery(plan)) {
      ScanNode scanNode = plan.getRootBlock().getNode(NodeType.SCAN);
      if (scanNode == null) {
        scanNode = plan.getRootBlock().getNode(NodeType.PARTITIONS_SCAN);
      }
      TableDesc desc = scanNode.getTableDesc();
      int maxRow = Integer.MAX_VALUE;
      if (plan.getRootBlock().hasNode(NodeType.LIMIT)) {
        LimitNode limitNode = plan.getRootBlock().getNode(NodeType.LIMIT);
        maxRow = (int) limitNode.getFetchFirstNum();
      }
      if (desc.getStats().getNumRows() == 0) {
        desc.getStats().setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
      }

      QueryId queryId = QueryIdFactory.newQueryId(context.getResourceManager().getSeedQueryId());

      NonForwardQueryResultScanner queryResultScanner =
          new NonForwardQueryResultScanner(context.getConf(), session.getSessionId(), queryId, scanNode, desc, maxRow);

      queryResultScanner.init();
      session.addNonForwardQueryResultScanner(queryResultScanner);

      responseBuilder.setQueryId(queryId.getProto());
      responseBuilder.setMaxRowNum(maxRow);
      responseBuilder.setTableDesc(desc.getProto());
      responseBuilder.setResult(buildOkRequestResult());

      // NonFromQuery indicates a form of 'select a, x+y;'
    } else if (PlannerUtil.checkIfNonFromQuery(plan)) {
      Target[] targets = plan.getRootBlock().getRawTargets();
      if (targets == null) {
        throw new PlanningException("No targets");
      }
      final Tuple outTuple = new VTuple(targets.length);
      for (int i = 0; i < targets.length; i++) {
        EvalNode eval = targets[i].getEvalTree();
        outTuple.put(i, eval.eval(null, null));
      }
      boolean isInsert = rootNode.getChild() != null && rootNode.getChild().getType() == NodeType.INSERT;
      if (isInsert) {
        InsertNode insertNode = rootNode.getChild();
        insertNonFromQuery(queryContext, insertNode, responseBuilder);
      } else {
        Schema schema = PlannerUtil.targetToSchema(targets);
        RowStoreUtil.RowStoreEncoder encoder = RowStoreUtil.createEncoder(schema);
        byte[] serializedBytes = encoder.toBytes(outTuple);
        SerializedResultSet.Builder serializedResBuilder = SerializedResultSet.newBuilder();
        serializedResBuilder.addSerializedTuples(ByteString.copyFrom(serializedBytes));
        serializedResBuilder.setSchema(schema.getProto());
        serializedResBuilder.setBytesNum(serializedBytes.length);

        responseBuilder.setResultSet(serializedResBuilder);
        responseBuilder.setMaxRowNum(1);
        responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
        responseBuilder.setResult(buildOkRequestResult());
      }
    } else { // it requires distributed execution. So, the query is forwarded to a query master.
      StoreType storeType = PlannerUtil.getStoreType(plan);
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

      response = submitForwardQuery(session, queryContext, sql, jsonExpr, rootNode, responseBuilder);
    }
    if (response == null) {
      responseBuilder.setSessionVars(ProtoUtil.convertFromMap(session.getAllVariables()));
      response = responseBuilder.build();
    }
    return response;
  }

  private SubmitQueryResponse submitForwardQuery(Session session, QueryContext queryContext, String sql,
                                                         String jsonExpr, LogicalRootNode rootNode,
                                                         SubmitQueryResponse.Builder responseBuilder) throws Exception {
    QueryJobManager queryJobManager = this.context.getQueryJobManager();
    QueryInfo queryInfo;

    queryInfo = queryJobManager.createNewQueryJob(session, queryContext, sql, jsonExpr, rootNode);

    if(queryInfo == null) {
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setResult(buildRequestResult(ResultCode.ERROR,
          "Fail starting QueryMaster.", null));
      LOG.error("Fail starting QueryMaster: " + sql);
    } else {
      responseBuilder.setIsForwarded(true);
      responseBuilder.setQueryId(queryInfo.getQueryId().getProto());
      responseBuilder.setResult(buildOkRequestResult());
      if(queryInfo.getQueryMasterHost() != null) {
        responseBuilder.setQueryMasterHost(queryInfo.getQueryMasterHost());
      }
      responseBuilder.setQueryMasterPort(queryInfo.getQueryMasterClientPort());
      LOG.info("Query " + queryInfo.getQueryId().toString() + "," + queryInfo.getSql() + "," +
          " is forwarded to " + queryInfo.getQueryMasterHost() + ":" + queryInfo.getQueryMasterPort());
    }
    responseBuilder.setQueryMasterPort(queryInfo.getQueryMasterClientPort());
    LOG.info("Query is forwarded to " + queryInfo.getQueryMasterHost() + ":" + queryInfo.getQueryMasterPort());

    responseBuilder.setSessionVars(ProtoUtil.convertFromMap(session.getAllVariables()));
    return responseBuilder.build();
  }

  private void insertNonFromQuery(QueryContext queryContext, InsertNode insertNode, SubmitQueryResponse.Builder responseBuilder)
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

      UpdateTableStatsProto.Builder builder = UpdateTableStatsProto.newBuilder();
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
    responseBuilder.setResult(buildOkRequestResult());
  }


  public QueryId updateQuery(QueryContext queryContext, String sql, boolean isJson) throws IOException, SQLException, PlanningException {
    try {
      LOG.info("SQL: " + sql);

      Expr expr;
      if (isJson) {
        expr = JsonHelper.fromJson(sql, Expr.class);
      } else {
        // parse the query
        expr = analyzer.parse(sql);
      }

      LogicalPlan plan = createLogicalPlan(queryContext, expr);
      LogicalRootNode rootNode = plan.getRootBlock().getRoot();

      if (!PlannerUtil.checkIfDDLPlan(rootNode)) {
        throw new SQLException("This is not update query:\n" + sql);
      } else {
        updateQuery(queryContext, rootNode.getChild());
        return QueryIdFactory.NULL_QUERY_ID;
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  private void updateQuery(QueryContext queryContext, LogicalNode root) throws IOException {
    switch (root.getType()) {
      case SET_SESSION:

      case CREATE_DATABASE:
        CreateDatabaseNode createDatabase = (CreateDatabaseNode) root;
        createDatabase(queryContext, createDatabase.getDatabaseName(), null, createDatabase.isIfNotExists());
        break;
      case DROP_DATABASE:
        DropDatabaseNode dropDatabaseNode = (DropDatabaseNode) root;
        dropDatabase(queryContext, dropDatabaseNode.getDatabaseName(), dropDatabaseNode.isIfExists());
        break;
      case CREATE_TABLE:
        CreateTableNode createTable = (CreateTableNode) root;
        createTable(queryContext, createTable, createTable.isIfNotExists());
        break;
      case DROP_TABLE:
        DropTableNode dropTable = (DropTableNode) root;
        dropTable(queryContext, dropTable.getTableName(), dropTable.isIfExists(), dropTable.isPurge());
        break;
      case ALTER_TABLESPACE:
        AlterTablespaceNode alterTablespace = (AlterTablespaceNode) root;
        alterTablespace(queryContext, alterTablespace);
        break;
      case ALTER_TABLE:
        AlterTableNode alterTable = (AlterTableNode) root;
        alterTable(queryContext,alterTable);
        break;
      case TRUNCATE_TABLE:
        TruncateTableNode truncateTable = (TruncateTableNode) root;
        truncateTable(queryContext, truncateTable);
        break;
      case CREATE_INDEX:
        // The catalog information for the created index is automatically updated when the query is successfully finished.
        // See the Query.CreateIndexHook class.
        break;
      case DROP_INDEX:
        DropIndexNode dropIndexNode = (DropIndexNode) root;
        dropIndex(queryContext, dropIndexNode);
        break;
      default:
        throw new InternalError("updateQuery cannot handle such query: \n" + root.toJson());
    }
  }

  private LogicalPlan createLogicalPlan(QueryContext queryContext, Expr expression) throws PlanningException {

    VerificationState state = new VerificationState();
    preVerifier.verify(queryContext, state, expression);
    if (!state.verified()) {
      StringBuilder sb = new StringBuilder();
      for (String error : state.getErrorMessages()) {
        sb.append(error).append("\n");
      }
      throw new VerifyException(sb.toString());
    }

    LogicalPlan plan = planner.createPlan(queryContext, expression);
    if (LOG.isDebugEnabled()) {
      LOG.debug("=============================================");
      LOG.debug("Non Optimized Query: \n" + plan.toString());
      LOG.debug("=============================================");
    }
    LOG.info("Non Optimized Query: \n" + plan.toString());
    optimizer.optimize(queryContext, plan);
    LOG.info("=============================================");
    LOG.info("Optimized Query: \n" + plan.toString());
    LOG.info("=============================================");

    annotatedPlanVerifier.verify(queryContext, state, plan);
    verifyInsertTableSchema(queryContext, state, plan);

    if (!state.verified()) {
      StringBuilder sb = new StringBuilder();
      for (String error : state.getErrorMessages()) {
        sb.append(error).append("\n");
      }
      throw new VerifyException(sb.toString());
    }

    return plan;
  }

  private void verifyInsertTableSchema(QueryContext queryContext, VerificationState state, LogicalPlan plan) {
    StoreType storeType = PlannerUtil.getStoreType(plan);
    if (storeType != null) {
      LogicalRootNode rootNode = plan.getRootBlock().getRoot();
      if (rootNode.getChild().getType() == NodeType.INSERT) {
        try {
          TableDesc tableDesc = PlannerUtil.getTableDesc(catalog, rootNode.getChild());
          InsertNode iNode = rootNode.getChild();
          Schema outSchema = iNode.getChild().getOutSchema();

          StorageManager.getStorageManager(queryContext.getConf(), storeType)
              .verifyInsertTableSchema(tableDesc, outSchema);
        } catch (Throwable t) {
          state.addVerification(t.getMessage());
        }
      }
    }
  }

  /**
   * Alter a given table
   */
  public void alterTablespace(final QueryContext queryContext, final AlterTablespaceNode alterTablespace) {

    final CatalogService catalog = context.getCatalog();
    final String spaceName = alterTablespace.getTablespaceName();

    AlterTablespaceProto.Builder builder = AlterTablespaceProto.newBuilder();
    builder.setSpaceName(spaceName);
    if (alterTablespace.getSetType() == AlterTablespaceSetType.LOCATION) {
      AlterTablespaceProto.AlterTablespaceCommand.Builder commandBuilder =
          AlterTablespaceProto.AlterTablespaceCommand.newBuilder();
      commandBuilder.setType(AlterTablespaceProto.AlterTablespaceType.LOCATION);
      commandBuilder.setLocation(AlterTablespaceProto.SetLocation.newBuilder().setUri(alterTablespace.getLocation()));
      commandBuilder.build();
      builder.addCommand(commandBuilder);
    } else {
      throw new RuntimeException("This 'ALTER TABLESPACE' is not supported yet.");
    }

    catalog.alterTablespace(builder.build());
  }

  /**
   * Alter a given table
   */
  public void alterTable(final QueryContext queryContext, final AlterTableNode alterTable) throws IOException {

    final CatalogService catalog = context.getCatalog();
    final String tableName = alterTable.getTableName();

    String databaseName;
    String simpleTableName;
    if (CatalogUtil.isFQTableName(tableName)) {
      String[] split = CatalogUtil.splitFQTableName(tableName);
      databaseName = split[0];
      simpleTableName = split[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleTableName = tableName;
    }
    final String qualifiedName = CatalogUtil.buildFQName(databaseName, simpleTableName);

    if (!catalog.existsTable(databaseName, simpleTableName)) {
      throw new NoSuchTableException(qualifiedName);
    }

    switch (alterTable.getAlterTableOpType()) {
      case RENAME_TABLE:
        if (!catalog.existsTable(databaseName, simpleTableName)) {
          throw new NoSuchTableException(alterTable.getTableName());
        }
        if (catalog.existsTable(databaseName, alterTable.getNewTableName())) {
          throw new AlreadyExistsTableException(alterTable.getNewTableName());
        }

        TableDesc desc = catalog.getTableDesc(databaseName, simpleTableName);

        if (!desc.isExternal()) { // if the table is the managed table
          Path oldPath = StorageUtil.concatPath(context.getConf().getVar(TajoConf.ConfVars.WAREHOUSE_DIR),
              databaseName, simpleTableName);
          Path newPath = StorageUtil.concatPath(context.getConf().getVar(TajoConf.ConfVars.WAREHOUSE_DIR),
              databaseName, alterTable.getNewTableName());
          FileSystem fs = oldPath.getFileSystem(context.getConf());

          if (!fs.exists(oldPath)) {
            throw new IOException("No such a table directory: " + oldPath);
          }
          if (fs.exists(newPath)) {
            throw new IOException("Already table directory exists: " + newPath);
          }

          fs.rename(oldPath, newPath);
        }
        catalog.alterTable(CatalogUtil.renameTable(qualifiedName, alterTable.getNewTableName(),
            AlterTableType.RENAME_TABLE));
        break;
      case RENAME_COLUMN:
        if (existColumnName(qualifiedName, alterTable.getNewColumnName())) {
          throw new ColumnNameAlreadyExistException(alterTable.getNewColumnName());
        }
        catalog.alterTable(CatalogUtil.renameColumn(qualifiedName, alterTable.getColumnName(),
            alterTable.getNewColumnName(), AlterTableType.RENAME_COLUMN));
        break;
      case ADD_COLUMN:
        if (existColumnName(qualifiedName, alterTable.getAddNewColumn().getSimpleName())) {
          throw new ColumnNameAlreadyExistException(alterTable.getAddNewColumn().getSimpleName());
        }
        catalog.alterTable(CatalogUtil.addNewColumn(qualifiedName, alterTable.getAddNewColumn(), AlterTableType.ADD_COLUMN));
        break;
      default:
        //TODO
    }
  }

  /**
   * Truncate table a given table
   */
  public void truncateTable(final QueryContext queryContext, final TruncateTableNode truncateTableNode)
      throws IOException {
    List<String> tableNames = truncateTableNode.getTableNames();
    final CatalogService catalog = context.getCatalog();

    String databaseName;
    String simpleTableName;

    List<TableDesc> tableDescList = new ArrayList<TableDesc>();
    for (String eachTableName: tableNames) {
      if (CatalogUtil.isFQTableName(eachTableName)) {
        String[] split = CatalogUtil.splitFQTableName(eachTableName);
        databaseName = split[0];
        simpleTableName = split[1];
      } else {
        databaseName = queryContext.getCurrentDatabase();
        simpleTableName = eachTableName;
      }
      final String qualifiedName = CatalogUtil.buildFQName(databaseName, simpleTableName);

      if (!catalog.existsTable(databaseName, simpleTableName)) {
        throw new NoSuchTableException(qualifiedName);
      }

      Path warehousePath = new Path(TajoConf.getWarehouseDir(context.getConf()), databaseName);
      TableDesc tableDesc = catalog.getTableDesc(databaseName, simpleTableName);
      Path tablePath = new Path(tableDesc.getPath());
      if (tablePath.getParent() == null ||
          !tablePath.getParent().toUri().getPath().equals(warehousePath.toUri().getPath())) {
        throw new IOException("Can't truncate external table:" + eachTableName + ", data dir=" + tablePath +
            ", warehouse dir=" + warehousePath);
      }
      tableDescList.add(tableDesc);
    }

    for (TableDesc eachTable: tableDescList) {
      Path path = new Path(eachTable.getPath());
      LOG.info("Truncate table: " + eachTable.getName() + ", delete all data files in " + path);
      FileSystem fs = path.getFileSystem(context.getConf());

      FileStatus[] files = fs.listStatus(path);
      if (files != null) {
        for (FileStatus eachFile: files) {
          fs.delete(eachFile.getPath(), true);
        }
      }
    }
  }

  public void dropIndex(final QueryContext queryContext, final DropIndexNode dropIndexNode) {
    String databaseName, simpleIndexName;
    if (CatalogUtil.isFQTableName(dropIndexNode.getIndexName())) {
      String [] splits = CatalogUtil.splitFQTableName(dropIndexNode.getIndexName());
      databaseName = splits[0];
      simpleIndexName = splits[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleIndexName = dropIndexNode.getIndexName();
    }

    if (!catalog.existIndexByName(databaseName, simpleIndexName)) {
      throw new NoSuchIndexException(simpleIndexName);
    }

    IndexDesc desc = catalog.getIndexByName(databaseName, simpleIndexName);

    if (!catalog.dropIndex(databaseName, simpleIndexName)) {
      LOG.info("Cannot drop index \"" + simpleIndexName + "\".");
      throw new CatalogException("Cannot drop index \"" + simpleIndexName + "\".");
    }

    Path indexPath = new Path(desc.getIndexPath());
    try {
      FileSystem fs = indexPath.getFileSystem(context.getConf());
      fs.delete(indexPath, true);
    } catch (IOException e) {
      throw new InternalError(e.getMessage());
    }

    LOG.info("Index " + simpleIndexName + " is dropped.");
  }

  private void checkIndexExistence(final QueryContext queryContext, final CreateIndexNode createIndexNode)
      throws IOException {
    String databaseName, simpleIndexName, qualifiedIndexName;
    if (CatalogUtil.isFQTableName(createIndexNode.getIndexName())) {
      String [] splits = CatalogUtil.splitFQTableName(createIndexNode.getIndexName());
      databaseName = splits[0];
      simpleIndexName = splits[1];
      qualifiedIndexName = createIndexNode.getIndexName();
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleIndexName = createIndexNode.getIndexName();
      qualifiedIndexName = CatalogUtil.buildFQName(databaseName, simpleIndexName);
    }

    if (catalog.existIndexByName(databaseName, simpleIndexName)) {
      throw new AlreadyExistsIndexException(qualifiedIndexName);
    }
  }

  public void createIndex(final QueryContext queryContext, final CreateIndexNode createIndexNode)
      throws IOException {
    String databaseName, simpleIndexName, qualifiedIndexName;
    if (CatalogUtil.isFQTableName(createIndexNode.getIndexName())) {
      String [] splits = CatalogUtil.splitFQTableName(createIndexNode.getIndexName());
      databaseName = splits[0];
      simpleIndexName = splits[1];
      qualifiedIndexName = createIndexNode.getIndexName();
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleIndexName = createIndexNode.getIndexName();
      qualifiedIndexName = CatalogUtil.buildFQName(databaseName, simpleIndexName);
    }

    if (catalog.existIndexByName(databaseName, simpleIndexName)) {
      throw new AlreadyExistsIndexException(qualifiedIndexName);
    }
  }

  private boolean existColumnName(String tableName, String columnName) {
    final TableDesc tableDesc = catalog.getTableDesc(tableName);
    return tableDesc.getSchema().containsByName(columnName) ? true : false;
  }

  private TableDesc createTable(QueryContext queryContext, CreateTableNode createTable, boolean ifNotExists) throws IOException {
    TableMeta meta;

    if (createTable.hasOptions()) {
      meta = CatalogUtil.newTableMeta(createTable.getStorageType(), createTable.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(createTable.getStorageType());
    }

    if(PlannerUtil.isFileStorageType(createTable.getStorageType()) && createTable.isExternal()){
      Preconditions.checkState(createTable.hasPath(), "ERROR: LOCATION must be given.");
    }

    return createTable(queryContext, createTable.getTableName(), createTable.getStorageType(),
        createTable.getTableSchema(), meta, createTable.getPath(), createTable.isExternal(),
        createTable.getPartitionMethod(), ifNotExists);
  }

  public TableDesc createTable(QueryContext queryContext, String tableName, StoreType storeType,
                               Schema schema, TableMeta meta, Path path, boolean isExternal,
                               PartitionMethodDesc partitionDesc, boolean ifNotExists) throws IOException {
    String databaseName;
    String simpleTableName;
    if (CatalogUtil.isFQTableName(tableName)) {
      String [] splitted = CatalogUtil.splitFQTableName(tableName);
      databaseName = splitted[0];
      simpleTableName = splitted[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleTableName = tableName;
    }
    String qualifiedName = CatalogUtil.buildFQName(databaseName, simpleTableName);

    boolean exists = catalog.existsTable(databaseName, simpleTableName);

    if (exists) {
      if (ifNotExists) {
        LOG.info("relation \"" + qualifiedName + "\" is already exists." );
        return catalog.getTableDesc(databaseName, simpleTableName);
      } else {
        throw new AlreadyExistsTableException(CatalogUtil.buildFQName(databaseName, tableName));
      }
    }

    TableDesc desc = new TableDesc(CatalogUtil.buildFQName(databaseName, simpleTableName),
        schema, meta, (path != null ? path.toUri(): null), isExternal);
    
    if (partitionDesc != null) {
      desc.setPartitionMethod(partitionDesc);
    }

    StorageManager.getStorageManager(queryContext.getConf(), storeType).createTable(desc, ifNotExists);

    if (catalog.createTable(desc)) {
      LOG.info("Table " + desc.getName() + " is created (" + desc.getStats().getNumBytes() + ")");
      return desc;
    } else {
      LOG.info("Table creation " + tableName + " is failed.");
      throw new CatalogException("Cannot create table \"" + tableName + "\".");
    }
  }

  public boolean createDatabase(@Nullable QueryContext queryContext, String databaseName,
                                @Nullable String tablespace,
                                boolean ifNotExists) throws IOException {

    String tablespaceName;
    if (tablespace == null) {
      tablespaceName = DEFAULT_TABLESPACE_NAME;
    } else {
      tablespaceName = tablespace;
    }

    // CREATE DATABASE IF NOT EXISTS
    boolean exists = catalog.existDatabase(databaseName);
    if (exists) {
      if (ifNotExists) {
        LOG.info("database \"" + databaseName + "\" is already exists." );
        return true;
      } else {
        throw new AlreadyExistsDatabaseException(databaseName);
      }
    }

    if (catalog.createDatabase(databaseName, tablespaceName)) {
      String normalized = databaseName;
      Path databaseDir = StorageUtil.concatPath(context.getConf().getVar(TajoConf.ConfVars.WAREHOUSE_DIR), normalized);
      FileSystem fs = databaseDir.getFileSystem(context.getConf());
      fs.mkdirs(databaseDir);
    }

    return true;
  }

  public boolean dropDatabase(QueryContext queryContext, String databaseName, boolean ifExists) {

    boolean exists = catalog.existDatabase(databaseName);
    if(!exists) {
      if (ifExists) { // DROP DATABASE IF EXISTS
        LOG.info("database \"" + databaseName + "\" does not exists." );
        return true;
      } else { // Otherwise, it causes an exception.
        throw new NoSuchDatabaseException(databaseName);
      }
    }

    if (queryContext.getCurrentDatabase().equals(databaseName)) {
      throw new RuntimeException("ERROR: Cannot drop the current open database");
    }

    boolean result = catalog.dropDatabase(databaseName);
    LOG.info("database " + databaseName + " is dropped.");
    return result;
  }

  /**
   * Drop a given named table
   *
   * @param tableName to be dropped
   * @param purge Remove all data if purge is true.
   */
  public boolean dropTable(QueryContext queryContext, String tableName, boolean ifExists, boolean purge) {
    CatalogService catalog = context.getCatalog();

    String databaseName;
    String simpleTableName;
    if (CatalogUtil.isFQTableName(tableName)) {
      String [] splitted = CatalogUtil.splitFQTableName(tableName);
      databaseName = splitted[0];
      simpleTableName = splitted[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleTableName = tableName;
    }
    String qualifiedName = CatalogUtil.buildFQName(databaseName, simpleTableName);

    boolean exists = catalog.existsTable(qualifiedName);
    if(!exists) {
      if (ifExists) { // DROP TABLE IF EXISTS
        LOG.info("relation \"" + qualifiedName + "\" is already exists." );
        return true;
      } else { // Otherwise, it causes an exception.
        throw new NoSuchTableException(qualifiedName);
      }
    }

    TableDesc tableDesc = catalog.getTableDesc(qualifiedName);
    catalog.dropTable(qualifiedName);

    if (purge) {
      try {
        StorageManager.getStorageManager(queryContext.getConf(),
            tableDesc.getMeta().getStoreType()).purgeTable(tableDesc);
      } catch (IOException e) {
        throw new InternalError(e.getMessage());
      }
    }

    LOG.info(String.format("relation \"%s\" is " + (purge ? " purged." : " dropped."), qualifiedName));
    return true;
  }

  private static RequestResult buildOkRequestResult() {
    return buildRequestResult(ResultCode.OK, null, null);
  }

  private static RequestResult buildRequestResult(ResultCode code,
                                                  @Nullable String errorMessage,
                                                  @Nullable String errorTrace) {
    RequestResult.Builder builder = RequestResult.newBuilder();
    builder.setResultCode(code);
    if (errorMessage != null) {
      builder.setErrorMessage(errorMessage);
    }
    if (errorTrace != null) {
      builder.setErrorTrace(errorTrace);
    }
    return builder.build();
  }

  public interface DistributedQueryHook {
    boolean isEligible(QueryContext queryContext, LogicalPlan plan);
    void hook(QueryContext queryContext, LogicalPlan plan) throws Exception;
  }

  public static class DistributedQueryHookManager {
    private List<DistributedQueryHook> hooks = new ArrayList<DistributedQueryHook>();
    public void addHook(DistributedQueryHook hook) {
      hooks.add(hook);
    }

    public void doHooks(QueryContext queryContext, LogicalPlan plan) {
      for (DistributedQueryHook hook : hooks) {
        if (hook.isEligible(queryContext, plan)) {
          try {
            hook.hook(queryContext, plan);
          } catch (Throwable t) {
            t.printStackTrace();
          }
        }
      }
    }
  }

  public class CreateTableHook implements DistributedQueryHook {

    @Override
    public boolean isEligible(QueryContext queryContext, LogicalPlan plan) {
      LogicalRootNode rootNode = plan.getRootBlock().getRoot();
      return rootNode.getChild().getType() == NodeType.CREATE_TABLE;
    }

    @Override
    public void hook(QueryContext queryContext, LogicalPlan plan) throws Exception {
      LogicalRootNode rootNode = plan.getRootBlock().getRoot();
      CreateTableNode createTableNode = rootNode.getChild();
      String [] splitted  = CatalogUtil.splitFQTableName(createTableNode.getTableName());
      String databaseName = splitted[0];
      String tableName = splitted[1];
      queryContext.setOutputTable(tableName);
      queryContext.setOutputPath(
          StorageUtil.concatPath(TajoConf.getWarehouseDir(context.getConf()), databaseName, tableName));
      if(createTableNode.getPartitionMethod() != null) {
        queryContext.setPartitionMethod(createTableNode.getPartitionMethod());
      }
      queryContext.setCreateTable();
    }
  }

  public static class InsertHook implements DistributedQueryHook {

    @Override
    public boolean isEligible(QueryContext queryContext, LogicalPlan plan) {
      return plan.getRootBlock().getRootType() == NodeType.INSERT;
    }

    @Override
  public void hook(QueryContext queryContext, LogicalPlan plan) throws Exception {
      queryContext.setInsert();

      InsertNode insertNode = plan.getRootBlock().getNode(NodeType.INSERT);

      // Set QueryContext settings, such as output table name and output path.
      // It also remove data files if overwrite is true.
      Path outputPath;
      if (insertNode.hasTargetTable()) { // INSERT INTO [TB_NAME]
        queryContext.setOutputTable(insertNode.getTableName());
        queryContext.setOutputPath(insertNode.getPath());
        if (insertNode.hasPartition()) {
          queryContext.setPartitionMethod(insertNode.getPartitionMethod());
        }
      } else { // INSERT INTO LOCATION ...
        // When INSERT INTO LOCATION, must not set output table.
        outputPath = insertNode.getPath();
        queryContext.setFileOutput();
        queryContext.setOutputPath(outputPath);
      }

      if (insertNode.isOverwrite()) {
        queryContext.setOutputOverwrite();
      }
    }
  }
}
