/**
 * 
 */
package tajo.engine.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.catalog.CatalogService;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableDesc;
import tajo.catalog.TableMeta;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.engine.*;
import tajo.engine.MasterInterfaceProtos.QueryStatus;
import tajo.engine.cluster.ClusterManager;
import tajo.engine.cluster.QueryManager;
import tajo.engine.cluster.WorkerCommunicator;
import tajo.engine.exception.EmptyClusterException;
import tajo.engine.exception.IllegalQueryStatusException;
import tajo.engine.exception.NoSuchQueryIdException;
import tajo.engine.exception.UnknownWorkerException;
import tajo.engine.parser.ParseTree;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.global.GlobalOptimizer;
import tajo.engine.planner.global.MasterPlan;
import tajo.engine.planner.global.ScheduleUnit;
import tajo.engine.planner.logical.*;
import tajo.storage.StorageManager;
import tajo.storage.StorageUtil;

import java.io.IOException;

/**
 * @author jihoon
 * 
 */
public class GlobalEngine implements EngineService {
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final TajoConf conf;
  private final CatalogService catalog;
  private final QueryAnalyzer analyzer;
  private final QueryContext.Factory factory;
  private final StorageManager sm;

  private GlobalPlanner globalPlanner;
  private GlobalOptimizer globalOptimizer;
  private WorkerCommunicator wc;
  private QueryManager qm;
  private ClusterManager cm;

  public GlobalEngine(final TajoConf conf, CatalogService cat,
      StorageManager sm, WorkerCommunicator wc, QueryManager qm,
      ClusterManager cm) throws IOException {
    this.conf = conf;
    this.catalog = cat;
    this.wc = wc;
    this.qm = qm;
    this.sm = sm;
    this.cm = cm;
    this.analyzer = new QueryAnalyzer(cat);
    this.factory = new QueryContext.Factory(catalog);

    this.globalPlanner = new GlobalPlanner(conf, this.sm, this.qm, this.catalog);
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
    plan = LogicalOptimizer.pushIndex(plan, sm);
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

      long totalSize = 0;
      try {
        totalSize = sm.calculateSize(createTable.getPath());
      } catch (IOException e) {
        LOG.error("Cannot calculate the size of the relation", e);
      }
      TableStat stat = new TableStat();
      stat.setNumBytes(totalSize);
      meta.setStat(stat);

      StorageUtil.writeTableMeta(conf, createTable.getPath(), meta);
      TableDesc desc = TCatUtil.newTableDesc(createTable.getTableName(), meta,
          createTable.getPath());
      catalog.addTable(desc);
      return desc.getId();
    } else {
      boolean hasStoreNode = false;
      if (root.getSubNode().getType() == ExprType.STORE) {
        hasStoreNode = true;
      }
      // other queries are executed by workers
      prepareQueryExecution(ctx);

      QueryId qid = QueryIdFactory.newQueryId();
      Query query = new Query(qid, querystr);
      qm.addQuery(query);
      query.setStatus(QueryStatus.QUERY_INITED);
      SubQueryId subId = QueryIdFactory.newSubQueryId(qid);
      SubQuery subQuery = new SubQuery(subId);
      qm.addSubQuery(subQuery);
      subQuery.setStatus(QueryStatus.QUERY_INITED);

      // build the master plan
      query.setStatus(QueryStatus.QUERY_INPROGRESS);
      subQuery.setStatus(QueryStatus.QUERY_INPROGRESS);
      MasterPlan globalPlan = globalPlanner.build(subId, plan);
      globalPlan = globalOptimizer.optimize(globalPlan.getRoot());
      
      /*QueryUnitScheduler queryUnitScheduler = new QueryUnitScheduler(
          conf, sm, cm, qm, wc, globalPlanner, globalPlan.getRoot());
      qm.addQueryUnitScheduler(subQuery, queryUnitScheduler);
      queryUnitScheduler.start();
      queryUnitScheduler.join();*/

      ScheduleUnitExecutor executor = new ScheduleUnitExecutor(conf,
          wc, globalPlanner, cm, qm, sm, globalPlan);
      executor.start();
      executor.join();

      finalizeQuery(query);

      if (hasStoreNode) {
        // create table queries are executed by the master
        StoreTableNode stn = (StoreTableNode) root.getSubNode();
        TableDesc desc = TCatUtil.newTableDesc(stn.getTableName(),
            sm.getTableMeta(globalPlan.getRoot().getOutputName()),
            sm.getTablePath(globalPlan.getRoot().getOutputName()));
        catalog.addTable(desc);
      }

      return sm.getTablePath(globalPlan.getRoot().getOutputName()).toString();
    }
  }

  public void finalizeQuery(Query query)
      throws IllegalQueryStatusException, UnknownWorkerException {
//    sendFinalize(query);
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

  /*public void sendFinalize(Query query) throws UnknownWorkerException {
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
  }*/

  private QueryStatus updateSubQueryStatus(SubQuery subQuery) {
    int i = 0, size = subQuery.getScheduleUnits().size();
    QueryStatus subQueryStatus = QueryStatus.QUERY_ABORTED;
    for (ScheduleUnit su : subQuery.getScheduleUnits()) {
      if (su.getStatus() != QueryStatus.QUERY_FINISHED) {
        break;
      }
      ++i;
    }
    if (i > 0 && i == size) {
      subQueryStatus = QueryStatus.QUERY_FINISHED;
    }
    subQuery.setStatus(subQueryStatus);
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
    query.setStatus(queryStatus);
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
   * @see EngineService#shutdown()
   */
  @Override
  public void shutdown() throws IOException {
    LOG.info(GlobalEngine.class.getName() + " is being stopped");
  }

}
