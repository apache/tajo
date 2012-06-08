/**
 * 
 */
package nta.engine.query;

import nta.catalog.CatalogService;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.engine.*;
import nta.engine.cluster.ClusterManager;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.GlobalOptimizer;
import nta.engine.planner.global.MasterPlan;
import nta.engine.planner.logical.CreateTableNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.storage.StorageManager;
import nta.storage.StorageUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

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
  
  public String executeQuery(String querystr) throws InterruptedException, IOException, NoSuchQueryIdException {
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
      // TODO: Queries should be maintained by the GlobalEngine or another class
      QueryId qid = QueryIdFactory.newQueryId();
      qm.addQuery(new Query(qid));
      LOG.info("=== Query " + qid + " is initialized");
      SubQueryId subId = QueryIdFactory.newSubQueryId(qid);
      SubQuery subQuery = new SubQuery(subId);
      LOG.info("=== SubQuery " + subId + " is initialized");
      qm.addSubQuery(subQuery);
      
      // build the master plan
      MasterPlan globalPlan = globalPlanner.build(subId, plan);
      globalPlan = globalOptimizer.optimize(globalPlan.getRoot());
      
      QueryUnitScheduler queryUnitScheduler = new QueryUnitScheduler(
          conf, sm, cm, qm, wc, globalPlanner, globalPlan.getRoot());
      qm.addQueryUnitScheduler(subQuery, queryUnitScheduler);
      queryUnitScheduler.start();
      queryUnitScheduler.join();

      return sm.getTablePath(globalPlan.getRoot().getOutputName()).toString();
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
