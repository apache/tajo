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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.exception.AlreadyExistsTableException;
import org.apache.tajo.catalog.exception.NoSuchTableException;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.ConstEval;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.exception.EmptyClusterException;
import org.apache.tajo.engine.exception.IllegalQueryStatusException;
import org.apache.tajo.engine.exception.NoSuchQueryIdException;
import org.apache.tajo.engine.exception.UnknownWorkerException;
import org.apache.tajo.engine.parser.HiveConverter;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.querymaster.QueryInfo;
import org.apache.tajo.master.querymaster.QueryJobManager;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.StorageUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tajo.ipc.ClientProtos.GetQueryStatusResponse;

public class GlobalEngine extends AbstractService {
  /** Class Logger */
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final MasterContext context;
  private final AbstractStorageManager sm;

  private SQLAnalyzer analyzer;
  private HiveConverter converter;
  private CatalogService catalog;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
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
      converter = new HiveConverter();
      planner = new LogicalPlanner(context.getCatalog());
      optimizer = new LogicalOptimizer();

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

  public GetQueryStatusResponse executeQuery(String sql)
      throws InterruptedException, IOException,
      NoSuchQueryIdException, IllegalQueryStatusException,
      UnknownWorkerException, EmptyClusterException {

    LOG.info("SQL: " + sql);
    QueryContext queryContext = new QueryContext();

    try {
      // setting environment variables
      String [] cmds = sql.split(" ");
      if(cmds != null) {
          if(cmds[0].equalsIgnoreCase("set")) {
              String[] params = cmds[1].split("=");
              context.getConf().set(params[0], params[1]);
              GetQueryStatusResponse.Builder responseBuilder = GetQueryStatusResponse.newBuilder();
              responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
              responseBuilder.setResultCode(ClientProtos.ResultCode.OK);
              responseBuilder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
              return responseBuilder.build();
          }
      }

      final boolean hiveQueryMode = context.getConf().getBoolVar(TajoConf.ConfVars.HIVE_QUERY_MODE);
      LOG.info("hive.query.mode:" + hiveQueryMode);

      if (hiveQueryMode) {
        queryContext.setHiveQueryMode();
      }

      Expr planningContext = hiveQueryMode ? converter.parse(sql) : analyzer.parse(sql);
      
      LogicalPlan plan = createLogicalPlan(planningContext);
      LogicalRootNode rootNode = plan.getRootBlock().getRoot();

      GetQueryStatusResponse.Builder responseBuilder = GetQueryStatusResponse.newBuilder();
      if (PlannerUtil.checkIfDDLPlan(rootNode)) {
        updateQuery(rootNode.getChild());
        responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
        responseBuilder.setResultCode(ClientProtos.ResultCode.OK);
        responseBuilder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
      } else {
        hookManager.doHooks(queryContext, plan);

        QueryJobManager queryJobManager = this.context.getQueryJobManager();
        QueryInfo queryInfo;

        queryInfo = queryJobManager.createNewQueryJob(queryContext, sql, rootNode);

        responseBuilder.setQueryId(queryInfo.getQueryId().getProto());
        responseBuilder.setResultCode(ClientProtos.ResultCode.OK);
        responseBuilder.setState(queryInfo.getQueryState());
        if(queryInfo.getQueryMasterHost() != null) {
          responseBuilder.setQueryMasterHost(queryInfo.getQueryMasterHost());
        }
        responseBuilder.setQueryMasterPort(queryInfo.getQueryMasterClientPort());
      }
      GetQueryStatusResponse response = responseBuilder.build();

      return response;
    } catch (Throwable t) {
      LOG.error("\nStack Trace:\n" + StringUtils.stringifyException(t));
      GetQueryStatusResponse.Builder responseBuilder = GetQueryStatusResponse.newBuilder();
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setResultCode(ClientProtos.ResultCode.ERROR);
      responseBuilder.setState(TajoProtos.QueryState.QUERY_ERROR);
      String errorMessage = t.getMessage();
      if (t.getMessage() == null) {
        errorMessage = StringUtils.stringifyException(t);
      }
      responseBuilder.setErrorMessage(errorMessage);
      return responseBuilder.build();
    }
  }

  public QueryId updateQuery(String sql) throws IOException, SQLException, PlanningException {
    LOG.info("SQL: " + sql);
    // parse the query
    Expr expr = analyzer.parse(sql);
    LogicalPlan plan = createLogicalPlan(expr);
    LogicalRootNode rootNode = (LogicalRootNode) plan.getRootBlock().getRoot();

    if (!PlannerUtil.checkIfDDLPlan(rootNode)) {
      throw new SQLException("This is not update query:\n" + sql);
    } else {
      updateQuery(rootNode.getChild());
      return QueryIdFactory.NULL_QUERY_ID;
    }
  }

  private boolean updateQuery(LogicalNode root) throws IOException {

    switch (root.getType()) {
      case CREATE_TABLE:
        CreateTableNode createTable = (CreateTableNode) root;
        createTable(createTable);
        return true;
      case DROP_TABLE:
        DropTableNode stmt = (DropTableNode) root;
        dropTable(stmt.getTableName());
        return true;

      default:
        throw new InternalError("updateQuery cannot handle such query: \n" + root.toJson());
    }
  }

  private LogicalPlan createLogicalPlan(Expr expression) throws PlanningException {

    LogicalPlan plan = planner.createPlan(expression);
    optimizer.optimize(plan);
    if (LOG.isDebugEnabled()) {
      LOG.debug("LogicalPlan:\n" + plan.getRootBlock().getRoot());
    }

    return plan;
  }

  private TableDesc createTable(CreateTableNode createTable) throws IOException {
    TableMeta meta;

    if (createTable.hasOptions()) {
      meta = CatalogUtil.newTableMeta(createTable.getSchema(),
          createTable.getStorageType(), createTable.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(createTable.getSchema(),
          createTable.getStorageType());
    }

    if(!createTable.isExternal()){
      Path tablePath = new Path(sm.getTableBaseDir(), createTable.getTableName().toLowerCase());
      createTable.setPath(tablePath);
    } else {
      Preconditions.checkState(createTable.hasPath(), "ERROR: LOCATION must be given.");
    }

    return createTable(createTable.getTableName(), meta, createTable.getPath());
  }

  public TableDesc createTable(String tableName, TableMeta meta, Path path) throws IOException {
    if (catalog.existsTable(tableName)) {
      throw new AlreadyExistsTableException(tableName);
    }

    FileSystem fs = path.getFileSystem(context.getConf());

    if(fs.exists(path) && fs.isFile(path)) {
      throw new IOException("ERROR: LOCATION must be a directory.");
    }

    long totalSize = 0;

    try {
      totalSize = sm.calculateSize(path);
    } catch (IOException e) {
      LOG.error("Cannot calculate the size of the relation", e);
    }

    TableStat stat = new TableStat();
    stat.setNumBytes(totalSize);
    meta.setStat(stat);

    TableDesc desc = CatalogUtil.newTableDesc(tableName, meta, path);
    StorageUtil.writeTableMeta(context.getConf(), path, meta);
    catalog.addTable(desc);

    LOG.info("Table " + desc.getName() + " is created (" + desc.getMeta().getStat().getNumBytes() + ")");

    return desc;
  }

  /**
   * Drop a given named table
   *
   * @param tableName to be dropped
   */
  public void dropTable(String tableName) {
    CatalogService catalog = context.getCatalog();

    if (!catalog.existsTable(tableName)) {
      throw new NoSuchTableException(tableName);
    }

    Path path = catalog.getTableDesc(tableName).getPath();
    catalog.deleteTable(tableName);

    try {

      FileSystem fs = path.getFileSystem(context.getConf());
      fs.delete(path, true);
    } catch (IOException e) {
      throw new InternalError(e.getMessage());
    }

    LOG.info("Table \"" + tableName + "\" is dropped.");
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
      if (plan.getRootBlock().hasStoreTableNode()) {
        StoreTableNode storeTableNode = plan.getRootBlock().getStoreTableNode();
        return storeTableNode.isCreatedTable();
      } else {
        return false;
      }
    }

    @Override
    public void hook(QueryContext queryContext, LogicalPlan plan) throws Exception {
      StoreTableNode storeTableNode = plan.getRootBlock().getStoreTableNode();
      String tableName = storeTableNode.getTableName();
      queryContext.setOutputTable(tableName);
      queryContext.setOutputPath(new Path(TajoConf.getWarehousePath(context.getConf()), tableName));
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

      InsertNode insertNode = plan.getRootBlock().getInsertNode();
      StoreTableNode storeNode;

      // Set QueryContext settings, such as output table name and output path.
      // It also remove data files if overwrite is true.
      String outputTableName;
      Path outputPath;
      if (insertNode.hasTargetTable()) { // INSERT INTO [TB_NAME]
        TableDesc desc = insertNode.getTargetTable();
        outputTableName = desc.getName();
        outputPath = desc.getPath();
        queryContext.setOutputTable(outputTableName);
      } else { // INSERT INTO LOCATION ...
        outputTableName = PlannerUtil.normalizeTableName(insertNode.getPath().getName());
        outputPath = insertNode.getPath();
        queryContext.setFileOutput();
      }

      storeNode = new StoreTableNode(plan.newPID(), outputTableName);
      queryContext.setOutputPath(outputPath);

      if (insertNode.isOverwrite()) {
        queryContext.setOutputOverwrite();
      }

      ////////////////////////////////////////////////////////////////////////////////////
      //             [TARGET TABLE]  [TARGET COLUMN]         [SUBQUERY Schema]           /
      // INSERT INTO    TB_NAME      (col1, col2)     SELECT    c1,   c2        FROM ... /
      ////////////////////////////////////////////////////////////////////////////////////
      LogicalNode subQuery = insertNode.getSubQuery();
      Schema subQueryOutSchema = subQuery.getOutSchema();

      if (insertNode.hasTargetTable()) { // if a target table is given, it computes the proper schema.
        Schema targetTableSchema = insertNode.getTargetTable().getMeta().getSchema();
        Schema targetProjectedSchema = insertNode.getTargetSchema();

        int [] targetColumnIds = new int[targetProjectedSchema.getColumnNum()];
        int idx = 0;
        for (Column column : targetProjectedSchema.getColumns()) {
          targetColumnIds[idx++] = targetTableSchema.getColumnId(column.getQualifiedName());
        }

        Target [] targets = new Target[targetTableSchema.getColumnNum()];
        boolean matched = false;
        for (int i = 0; i < targetTableSchema.getColumnNum(); i++) {
          Column column = targetTableSchema.getColumn(i);
          for (int j = 0; j < targetColumnIds.length; j++) {
            if (targetColumnIds[j] == i) {
              Column outputColumn = subQueryOutSchema.getColumn(j);
              targets[i] = new Target(new FieldEval(outputColumn), column.getColumnName());
              matched = true;
              break;
            }
          }
          if (!matched) {
            targets[i] = new Target(new ConstEval(NullDatum.get()), column.getColumnName());
          }
          matched = false;
        }


        ProjectionNode projectionNode = new ProjectionNode(plan.newPID(), targets);
        projectionNode.setInSchema(insertNode.getSubQuery().getOutSchema());
        projectionNode.setOutSchema(PlannerUtil.targetToSchema(targets));
        List<LogicalPlan.QueryBlock> blocks = plan.getChildBlocks(plan.getRootBlock());
        projectionNode.setChild(blocks.get(0).getRoot());

        storeNode.setOutSchema(projectionNode.getOutSchema());
        storeNode.setInSchema(projectionNode.getOutSchema());
        storeNode.setChild(projectionNode);
      } else {
        storeNode.setOutSchema(subQueryOutSchema);
        storeNode.setInSchema(subQueryOutSchema);
        List<LogicalPlan.QueryBlock> childBlocks = plan.getChildBlocks(plan.getRootBlock());
        storeNode.setChild(childBlocks.get(0).getRoot());
      }

      if (insertNode.hasStorageType()) {
        storeNode.setStorageType(insertNode.getStorageType());
      }
      if (insertNode.hasOptions()) {
        storeNode.setOptions(insertNode.getOptions());
      }

      // find a subquery query of insert node and merge root block and subquery into one query block.
      PlannerUtil.replaceNode(plan.getRootBlock().getRoot(), storeNode, NodeType.INSERT);
      plan.getRootBlock().refresh();
      LogicalPlan.QueryBlock subBlock = plan.getBlock(insertNode.getSubQuery());
      plan.removeBlock(subBlock);
    }
  }
}
