/**
 * 
 */
package nta.engine.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nta.catalog.CatalogService;
import nta.catalog.TableDesc;
import nta.engine.EngineService;
import nta.engine.Query;
import nta.engine.QueryContext;
import nta.engine.QueryId;
import nta.engine.QueryIdFactory;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitScheduler;
import nta.engine.SubQuery;
import nta.engine.SubQueryId;
import nta.engine.cluster.ClusterManager;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.LogicalQueryUnitGraph;
import nta.engine.planner.logical.LogicalNode;
import nta.storage.StorageManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * @author jihoon
 * 
 */
public class GlobalEngine implements EngineService {
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private Configuration conf;
  private final CatalogService catalog;
  private final QueryAnalyzer analyzer;
  private final QueryContext.Factory factory;
  private final StorageManager sm;

  private GlobalQueryPlanner globalPlanner;
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

    this.globalPlanner = new GlobalQueryPlanner(this.sm);
  }

  public void createTable(TableDesc meta) throws IOException {
    catalog.addTable(meta);
  }
  
  public String executeQuery(String querystr) throws Exception {
    LOG.info("* issued query: " + querystr);
    // build the logical plan
    QueryContext ctx = factory.create();
    ParseTree tree = (QueryBlock) analyzer.parse(ctx, querystr);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, tree);
    LogicalOptimizer.optimize(ctx, plan);
    LOG.info("* logical plan:\n" + plan);

    QueryId qid = QueryIdFactory.newQueryId();
    qm.addQuery(new Query(qid));
    SubQueryId subId = QueryIdFactory.newSubQueryId();
    SubQuery subQuery = new SubQuery(subId);
    qm.addSubQuery(subQuery);
    // build the global plan
    LogicalQueryUnitGraph globalPlan = globalPlanner.build(subId, plan);
    
    QueryUnitScheduler queryUnitScheduler = new QueryUnitScheduler(
        conf, sm, cm, qm, wc, globalPlanner, globalPlan.getRoot());
    qm.addQueryUnitScheduler(subQuery, queryUnitScheduler);
    queryUnitScheduler.start();
    queryUnitScheduler.join();

    return globalPlan.getRoot().getOutputName();
  }
  
  public LogicalQueryUnitGraph testQuery(String querystr) throws Exception {
    LOG.info("* issued query: " + querystr);
    // build the logical plan
    QueryContext ctx = factory.create();
    ParseTree tree = (QueryBlock) analyzer.parse(ctx, querystr);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, tree);
    LogicalOptimizer.optimize(ctx, plan);
    LOG.info("* logical plan:\n" + plan);

    QueryId qid = QueryIdFactory.newQueryId();
    qm.addQuery(new Query(qid));
    SubQueryId subId = QueryIdFactory.newSubQueryId();
    SubQuery subQuery = new SubQuery(subId);
    qm.addSubQuery(subQuery);
    // build the global plan
    return globalPlanner.build(subId, plan);
  }

  public Map<QueryUnitId, Float> getProgress(SubQueryId subqid) {
    Map<QueryUnitId, Float> progressMap = new HashMap<QueryUnitId, Float>();
    
    
    
    return progressMap;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see nta.engine.EngineService#init()
   */
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
