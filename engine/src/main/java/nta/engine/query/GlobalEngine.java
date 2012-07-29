/**
 * 
 */
package nta.engine.query;

import java.io.IOException;

import nta.catalog.CatalogService;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.engine.*;
import nta.engine.MasterInterfaceProtos.*;
import nta.engine.cluster.ClusterManager;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.exception.EmptyClusterException;
import nta.engine.exception.IllegalQueryStatusException;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.exception.UnknownWorkerException;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.GlobalOptimizer;
import nta.engine.planner.global.MasterPlan;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.logical.CreateTableNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.storage.StorageManager;
import nta.storage.StorageUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * @author jihoon
 * 
 */
public class GlobalEngine implements EngineService {
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final Configuration conf;
  private final CatalogService catalog;
  private final QueryAnalyzer analyzer;
  private final QueryContext.Factory factory;
  private final StorageManager sm;

  private GlobalPlanner globalPlanner;
  private GlobalOptimizer globalOptimizer;
  private WorkerCommunicator wc;
  private QueryManager qm;
  private ClusterManager cm;
  
  public GlobalEngine(Configuration conf, CatalogService cat,
      StorageManager sm, WorkerCommunicator wc,
      QueryManager qm, ClusterManager cm)
      throws IOException {
    this.conf = conf;
    this.catalog = cat;
    this.wc = wc;
    this.qm = qm;
    this.sm = sm;
    this.cm = cm;
    this.analyzer = new QueryAnalyzer(cat);
    this.factory = new QueryContext.Factory(catalog);

    this.globalPlanner = new GlobalPlanner(this.sm, this.qm, 
        this.catalog);
    this.globalOptimizer = new GlobalOptimizer();
  }

  public void createTable(TableDesc meta) throws IOException {
    catalog.addTable(meta);
  }
  
  public String executeQuery(String querystr)
      throws InterruptedException, IOException,
      NoSuchQueryIdException, IllegalQueryStatusException,
      UnknownWorkerException, EmptyClusterException {
    LOG.info("* issued query: " + querystr);
    // build the logical plan
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, querystr);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, tree);
    plan = LogicalOptimizer.optimize(ctx, plan);
    LOG.info("* logical plan:\n" + plan);
    
    LogicalRootNode root = (LogicalRootNode) plan;
    if (root.getSubNode().getType() == ExprType.CREATE_TABLE) {
      // create table queries are executed by the master
      CreateTableNode createTable = (CreateTableNode) root.getSubNode();
      TableMeta meta;
      if (createTable.hasOptions()) {
        meta = TCatUtil.newTableMeta(createTable.getSchema(), 
            createTable.getStoreType(), createTable.getOptions());
      } else {
        meta = TCatUtil.newTableMeta(createTable.getSchema(), 
            createTable.getStoreType());
      }
      StorageUtil.writeTableMeta(conf, createTable.getPath(), meta);
      TableDesc desc = TCatUtil.newTableDesc(createTable.getTableName(), meta, 
          createTable.getPath());
      catalog.addTable(desc);
      return desc.getId();
    } else {
      // other queries are executed by workers
      prepareQueryExecution(ctx);
      
      QueryId qid = QueryIdFactory.newQueryId();
      Query query = new Query(qid, querystr);
      qm.addQuery(query);
      qm.updateQueryStatus(qid, QueryStatus.QUERY_INITED);
      qm.updateQueryStatus(qid, QueryStatus.QUERY_PENDING);
      SubQueryId subId = QueryIdFactory.newSubQueryId(qid);
      SubQuery subQuery = new SubQuery(subId);
      qm.addSubQuery(subQuery);
      qm.updateSubQueryStatus(subId, QueryStatus.QUERY_INITED);
      qm.updateSubQueryStatus(subId, QueryStatus.QUERY_PENDING);
      
      // build the master plan
      qm.updateQueryStatus(qid, QueryStatus.QUERY_INPROGRESS);
      qm.updateSubQueryStatus(subId, QueryStatus.QUERY_INPROGRESS);
      MasterPlan globalPlan = globalPlanner.build(subId, plan);
      globalPlan = globalOptimizer.optimize(globalPlan.getRoot());
      
      QueryUnitScheduler queryUnitScheduler = new QueryUnitScheduler(
          conf, sm, cm, qm, wc, globalPlanner, globalPlan.getRoot());
      qm.addQueryUnitScheduler(subQuery, queryUnitScheduler);
      queryUnitScheduler.start();
      queryUnitScheduler.join();

      finalizeQuery(query);

      return sm.getTablePath(globalPlan.getRoot().getOutputName()).toString();
    }
  }

  public void finalizeQuery(Query query)
      throws IllegalQueryStatusException, UnknownWorkerException {
    sendFinalize(query);
    QueryStatus status = updateQueryStatus(query);
    switch (status) {
      case QUERY_FINISHED:
        LOG.info("Query " + query.getId() + " is finished.");
        break;
      case QUERY_ABORTED:
        LOG.info("Query " + query.getId() + " is aborted!!");
        break;
      case QUERY_KILLED:
        LOG.info("Query " + query.getId() + " is killed!!");
        break;
      default:
        throw new IllegalQueryStatusException(
            "Illegal final status of query " +
                query.getId() + ": " + status);
    }
  }

  public void sendFinalize(Query query) throws UnknownWorkerException {
    Command.Builder cmd = Command.newBuilder();
    for (SubQuery subQuery : query.getSubQueries()) {
      for (ScheduleUnit scheduleUnit : subQuery.getScheduleUnits()) {
        for (QueryUnit queryUnit : scheduleUnit.getQueryUnits()) {
          cmd.setId(queryUnit.getId().getProto()).setType(CommandType.FINALIZE);
          wc.requestCommand(queryUnit.getHost(),
              CommandRequestProto.newBuilder().addCommand(cmd.build()).build());
        }
      }
    }
  }

  private QueryStatus updateSubQueryStatus(SubQuery subQuery) {
    int i = 0, size = subQuery.getScheduleUnits().size();
    QueryStatus subQueryStatus = QueryStatus.QUERY_ABORTED;
    for (ScheduleUnit su : subQuery.getScheduleUnits()) {
      if (qm.getScheduleUnitStatus(su)
        != QueryStatus.QUERY_FINISHED) {
        break;
      }
      ++i;
    }
    if (i > 0 && i == size) {
      subQueryStatus = QueryStatus.QUERY_FINISHED;
    }
    qm.updateSubQueryStatus(subQuery.getId(),
        subQueryStatus);
    return subQueryStatus;
  }

  private QueryStatus updateQueryStatus(Query query) {
    int i = 0, size = query.getSubQueries().size();
    QueryStatus queryStatus = QueryStatus.QUERY_ABORTED;
    for (SubQuery sq : query.getSubQueries()) {
      if (updateSubQueryStatus(sq)
        != QueryStatus.QUERY_FINISHED) {
        break;
      }
      ++i;
    }
    if (i > 0 && i == size) {
      queryStatus = QueryStatus.QUERY_FINISHED;
    }
    qm.updateQueryStatus(query.getId(), queryStatus);
    return queryStatus;
  }

  private void prepareQueryExecution(QueryContext ctx) throws IOException {
    cm.updateOnlineWorker();
    for (String table : ctx.getInputTables()) {
      cm.updateFragmentServingInfo2(table);
    }
  }
  
  @Override
  public void init() throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see nta.engine.EngineService#shutdown()
   */
  @Override
  public void shutdown() throws IOException {
    LOG.info(GlobalEngine.class.getName() + " is being stopped");
  }

}
