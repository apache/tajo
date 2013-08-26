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
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.exception.AlreadyExistsTableException;
import org.apache.tajo.catalog.exception.NoSuchTableException;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.engine.exception.EmptyClusterException;
import org.apache.tajo.engine.exception.IllegalQueryStatusException;
import org.apache.tajo.engine.exception.NoSuchQueryIdException;
import org.apache.tajo.engine.exception.UnknownWorkerException;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.global.GlobalOptimizer;
import org.apache.tajo.engine.planner.logical.CreateTableNode;
import org.apache.tajo.engine.planner.logical.DropTableNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalRootNode;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.querymaster.QueryInfo;
import org.apache.tajo.master.querymaster.QueryJobManager;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageUtil;

import java.io.IOException;
import java.sql.SQLException;

@SuppressWarnings("unchecked")
public class GlobalEngine extends AbstractService {
  /** Class Logger */
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final MasterContext context;
  private final StorageManager sm;

  private SQLAnalyzer analyzer;
  private CatalogService catalog;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private GlobalPlanner globalPlanner;
  private GlobalOptimizer globalOptimizer;

  public GlobalEngine(final MasterContext context)
      throws IOException {
    super(GlobalEngine.class.getName());
    this.context = context;
    this.catalog = context.getCatalog();
    this.sm = context.getStorageManager();
  }

  public void start() {
    try  {
      analyzer = new SQLAnalyzer();
      planner = new LogicalPlanner(context.getCatalog());
      optimizer = new LogicalOptimizer();

      globalPlanner = new GlobalPlanner(context.getConf(), sm, context.getEventHandler());

      globalOptimizer = new GlobalOptimizer();
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    super.start();
  }

  public void stop() {
    super.stop();
  }

  public ClientProtos.GetQueryStatusResponse executeQuery(String sql)
      throws InterruptedException, IOException,
      NoSuchQueryIdException, IllegalQueryStatusException,
      UnknownWorkerException, EmptyClusterException {

    LOG.info("SQL: " + sql);
    // parse the query
    Expr planningContext = analyzer.parse(sql);
    LogicalRootNode plan = (LogicalRootNode) createLogicalPlan(planningContext);

    ClientProtos.GetQueryStatusResponse.Builder responseBuilder = ClientProtos.GetQueryStatusResponse.newBuilder();

    if (PlannerUtil.checkIfDDLPlan(plan)) {
      updateQuery(plan.getChild());

      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setResultCode(ClientProtos.ResultCode.OK);
      responseBuilder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
    } else {
      QueryJobManager queryJobManager = context.getQueryJobManager();
      QueryInfo queryInfo = null;
      try {
        queryInfo = queryJobManager.createNewQueryJob(sql, plan);
      } catch (Exception e) {
        responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
        responseBuilder.setResultCode(ClientProtos.ResultCode.ERROR);
        responseBuilder.setState(TajoProtos.QueryState.QUERY_ERROR);
        responseBuilder.setErrorMessage(StringUtils.stringifyException(e));

        return responseBuilder.build();
      }

      //queryJobManager.getEventHandler().handle(new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_START, queryInfo));

      responseBuilder.setQueryId(queryInfo.getQueryId().getProto());
      responseBuilder.setResultCode(ClientProtos.ResultCode.OK);
      responseBuilder.setState(queryInfo.getQueryState());
      if(queryInfo.getQueryMasterHost() != null) {
        responseBuilder.setQueryMasterHost(queryInfo.getQueryMasterHost());
      }
      responseBuilder.setQueryMasterPort(queryInfo.getQueryMasterClientPort());
    }

    ClientProtos.GetQueryStatusResponse response = responseBuilder.build();
    return response;
  }

  public QueryId updateQuery(String sql) throws IOException, SQLException {
    LOG.info("SQL: " + sql);
    // parse the query
    Expr planningContext = analyzer.parse(sql);
    LogicalRootNode plan = (LogicalRootNode) createLogicalPlan(planningContext);

    if (!PlannerUtil.checkIfDDLPlan(plan)) {
      throw new SQLException("This is not update query:\n" + sql);
    } else {
      updateQuery(plan.getChild());
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

  private LogicalNode createLogicalPlan(Expr expression) throws IOException {

    LogicalPlan plan = planner.createPlan(expression);
    LogicalNode optimizedPlan = null;
    try {
      optimizedPlan = optimizer.optimize(plan);
    } catch (PlanningException e) {
      LOG.error(e.getMessage(), e);
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("LogicalPlan:\n" + plan.getRootBlock().getRoot());
    }

    return optimizedPlan;
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
}
