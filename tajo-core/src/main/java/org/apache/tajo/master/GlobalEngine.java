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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.JsonHelper;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.*;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.exec.DDLExecutor;
import org.apache.tajo.master.exec.QueryExecutor;
import org.apache.tajo.metrics.Master;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.InsertNode;
import org.apache.tajo.plan.logical.LogicalRootNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.verifier.*;
import org.apache.tajo.session.Session;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.CommonTestingUtil;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;

public class GlobalEngine extends AbstractService {
  /** Class Logger */
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final MasterContext context;

  private SQLAnalyzer analyzer;
  private CatalogService catalog;
  private PreLogicalPlanVerifier preVerifier;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private LogicalPlanVerifier annotatedPlanVerifier;
  private PostLogicalPlanVerifier postLogicalPlanVerifier;

  private QueryExecutor queryExecutor;
  private DDLExecutor ddlExecutor;

  public GlobalEngine(final MasterContext context) {
    super(GlobalEngine.class.getName());
    this.context = context;
    this.catalog = context.getCatalog();

    this.ddlExecutor = new DDLExecutor(context);
    this.queryExecutor = new QueryExecutor(context, ddlExecutor);
  }

  public void start() {
    try  {
      analyzer = new SQLAnalyzer();
      preVerifier = new PreLogicalPlanVerifier(context.getCatalog());
      planner = new LogicalPlanner(context.getCatalog(), TablespaceManager.getInstance());
      // Access path rewriter is enabled only in QueryMasterTask
      optimizer = new LogicalOptimizer(context.getConf(), context.getCatalog());
      annotatedPlanVerifier = new LogicalPlanVerifier();
      postLogicalPlanVerifier = new PostLogicalPlanVerifier();
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw new RuntimeException(t);
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

  public DDLExecutor getDDLExecutor() {
    return ddlExecutor;
  }

  public QueryExecutor getQueryExecutor() {
    return queryExecutor;
  }

  private QueryContext createQueryContext(Session session) {
    QueryContext newQueryContext =  new QueryContext(context.getConf(), session);

    // Set default space uri and its root uri
    newQueryContext.setDefaultSpaceUri(TablespaceManager.getDefault().getUri());
    newQueryContext.setDefaultSpaceRootUri(TablespaceManager.getDefault().getRootUri());

    String tajoTest = System.getProperty(CommonTestingUtil.TAJO_TEST_KEY);
    if (tajoTest != null && tajoTest.equalsIgnoreCase(CommonTestingUtil.TAJO_TEST_TRUE)) {
      newQueryContext.putAll(CommonTestingUtil.getSessionVarsForTest());
    }

    // Set queryCache in session
    int queryCacheSize = context.getConf().getIntVar(TajoConf.ConfVars.QUERY_SESSION_QUERY_CACHE_SIZE);
    if (queryCacheSize > 0 && session.getQueryCache() == null) {
      Weigher<String, Expr> weighByLength = new Weigher<String, Expr>() {
        public int weigh(String key, Expr expr) {
          return key.length();
        }
      };
      LoadingCache<String, Expr> cache = CacheBuilder.newBuilder()
        .maximumWeight(queryCacheSize * 1024)
        .weigher(weighByLength)
        .expireAfterAccess(1, TimeUnit.HOURS)
        .build(new CacheLoader<String, Expr>() {
          public Expr load(String sql) throws SQLSyntaxError {
            return analyzer.parse(sql);
          }
        });
      session.setQueryCache(cache);
    }
    return newQueryContext;
  }

  public SubmitQueryResponse executeQuery(Session session, String query, boolean isJson) {
    LOG.info("Query: " + query);
    QueryContext queryContext = createQueryContext(session);
    Expr planningContext;

    try {
      context.getMetrics().counter(Master.Query.SUBMITTED).inc();

      if (isJson) {
        planningContext = buildExpressionFromJson(query);
      } else {
        planningContext = buildExpressionFromSql(query, session);
      }

      String jsonExpr = planningContext.toJson();
      LogicalPlan plan = createLogicalPlan(queryContext, planningContext);
      SubmitQueryResponse response = queryExecutor.execute(queryContext, session, query, jsonExpr, plan);
      return response;


    } catch (Throwable t) {
      ExceptionUtil.printStackTraceIfError(LOG, t);

      context.getMetrics().counter(Master.Query.ERROR).inc();

      SubmitQueryResponse.Builder responseBuilder = SubmitQueryResponse.newBuilder();
      responseBuilder.setUserName(queryContext.get(SessionVars.USERNAME));
      responseBuilder.setQueryId(QueryIdFactory.NULL_QUERY_ID.getProto());
      responseBuilder.setState(ReturnStateUtil.returnError(t));
      return responseBuilder.build();
    }
  }

  public Expr buildExpressionFromJson(String json) {
    return JsonHelper.fromJson(json, Expr.class);
  }

  public Expr buildExpressionFromSql(String sql, Session session) throws TajoException {
    try {

      if (session.getQueryCache() == null) {
        return analyzer.parse(sql);

      } else {
        try {
          return (Expr) session.getQueryCache().get(sql.trim()).clone();
        } catch (ExecutionException e) {
          throw e.getCause();
        }
      }

    } catch (Throwable t) {
      if (t instanceof TajoException) {
        throw (TajoException)t;
      } else if (t instanceof TajoRuntimeException) {
        throw (TajoException)t.getCause();
      } else {
        throw new TajoInternalError(t);
      }
    }
  }

  public QueryId updateQuery(QueryContext queryContext, String sql, boolean isJson) throws Throwable {
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
        ddlExecutor.execute(queryContext, plan);
        return QueryIdFactory.NULL_QUERY_ID;
      }
    } catch (Throwable e) {
      ExceptionUtil.printStackTraceIfError(LOG, e);
      throw e;
    }
  }

  private LogicalPlan createLogicalPlan(QueryContext queryContext, Expr expression) throws Throwable {

    VerificationState state = new VerificationState();
    preVerifier.verify(queryContext, state, expression);
    if (!state.verified()) {
      for (Throwable error : state.getErrors()) {
        throw error;
      }
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

    annotatedPlanVerifier.verify(state, plan);
    verifyInsertTableSchema(state, plan);

    if (!state.verified()) {
      for (Throwable error : state.getErrors()) {
        throw error;
      }
    }

    postLogicalPlanVerifier.verify(queryContext.getLong(SessionVars.BROADCAST_CROSS_JOIN_THRESHOLD), state, plan);
    if (!state.verified()) {
      for (Throwable error : state.getErrors()) {
        throw error;
      }
    }

    return plan;
  }

  private void verifyInsertTableSchema(VerificationState state, LogicalPlan plan) {
    String storeType = PlannerUtil.getStoreType(plan);
    if (storeType != null) {
      LogicalRootNode rootNode = plan.getRootBlock().getRoot();
      if (rootNode.getChild().getType() == NodeType.INSERT) {
        try {
          TableDesc tableDesc = PlannerUtil.getTableDesc(catalog, rootNode.getChild());
          InsertNode iNode = rootNode.getChild();
          Schema outSchema = iNode.getChild().getOutSchema();

          TablespaceManager.get(tableDesc.getUri()).verifySchemaToWrite(tableDesc, outSchema);

        } catch (TajoException t) {
          state.addVerification(t);
        } catch (TajoRuntimeException t) {
          state.addVerification(t);
        } catch (Throwable t) {
          state.addVerification(SyntaxErrorUtil.makeSyntaxError(t.getMessage()));
        }
      }
    }
  }
}
