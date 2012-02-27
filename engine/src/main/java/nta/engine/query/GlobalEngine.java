/**
 * 
 */
package nta.engine.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import nta.catalog.CatalogService;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.EngineService;
import nta.engine.NtaEngineMaster;
import nta.engine.QueryContext;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.SubQueryId;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.exception.NTAQueryException;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.LogicalQueryUnit;
import nta.engine.planner.global.LogicalQueryUnit.Phase;
import nta.engine.planner.global.LogicalQueryUnitGraph;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.logical.LogicalNode;
import nta.storage.StorageManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

/**
 * @author jihoon
 * 
 */
public class GlobalEngine implements EngineService {
  private Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final Configuration conf;
  private final CatalogService catalog;
  private final QueryAnalyzer analyzer;
  private final StorageManager storageManager;
  private final QueryContext.Factory factory;
  private final StorageManager sm;

  private GlobalQueryPlanner globalPlanner;
  private WorkerCommunicator wc;
  private QueryManager qm;
  
  private NtaEngineMaster master;
  
  private List<QueryUnit> executionQueue = new ArrayList<QueryUnit>();
  private List<QueryUnit> waitQueue = new ArrayList<QueryUnit>();
  private Random rand = new Random();
  
  public GlobalEngine(Configuration conf, CatalogService cat,
      StorageManager sm, WorkerCommunicator wc,
      QueryManager qm, NtaEngineMaster master)
      throws IOException {
    this.conf = conf;
    this.catalog = cat;
    this.storageManager = sm;
    this.wc = wc;
    this.qm = qm;
    this.master = master;
    this.analyzer = new QueryAnalyzer(cat);
    this.factory = new QueryContext.Factory(catalog);
    this.sm = new StorageManager(conf);

    this.globalPlanner = new GlobalQueryPlanner(this.sm);
  }

  public void createTable(TableDesc meta) throws IOException {
    catalog.addTable(meta);
  }
  
  private void recursiveExecuteQueryUnit(LogicalQueryUnit unit) 
      throws Exception {
    if (unit.hasPrevQuery()) {
      Iterator<LogicalQueryUnit> it = unit.getPrevIterator();
      while (it.hasNext()) {
        recursiveExecuteQueryUnit(it.next());
      }
    }
    
    LOG.info("Table path " + sm.getTablePath(unit.getOutputName()).toString()
        + " is initialized for " + unit.getOutputName());
    if (unit.getPhase() == Phase.MAP) {
      Path tablePath = sm.getTablePath(unit.getOutputName());
      sm.getFileSystem().mkdirs(tablePath);
    } else {
      sm.initTableBase(TCatUtil.newTableMeta(unit.getOutputSchema(), StoreType.CSV), 
          unit.getOutputName());
    }
    
    // TODO: adjust the number of localization
    QueryUnit[] units = globalPlanner.localize(unit, master.getOnlineServer().size());
    for (QueryUnit q : units) {
      q.setHost(getRandomHost());
      executionQueue.add(q);
    }
    while (!executionQueue.isEmpty()) {
      QueryUnit q = executionQueue.remove(0);
      waitQueue.add(q);
      QueryUnitRequest request = new QueryUnitRequestImpl(q.getId(), q.getFragments(), 
          q.getOutputName(), false, q.getLogicalPlan().toJSON());
      wc.requestQueryUnit(q.getHost(), request.getProto());
      LOG.info("QueryUnitRequest " + q.getId() + " is sent to " + (q.getHost()));
      LOG.info("QueryStep's output name " + q.getStoreTableNode().getTableName());
    }
    
    waitForFinishQueryUnits();
    waitQueue.clear();
  }
  
  public String executeQuery(SubQueryId subQueryId, String querystr) throws Exception {
    LOG.info("* issued query: " + querystr);
    // build the logical plan
    QueryContext ctx = factory.create();
    ParseTree tree = (QueryBlock) analyzer.parse(ctx, querystr);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, tree);
    LogicalOptimizer.optimize(ctx, plan);
    LOG.info("* logical plan:\n" + plan);

    // build the global plan
    LogicalQueryUnitGraph globalPlan = globalPlanner.build(subQueryId, plan);

    long before = System.currentTimeMillis();
    
    // plan을 후위로 순회하면서 localize한 후 QueryUnit 실행
    recursiveExecuteQueryUnit(globalPlan.getRoot());

    long after = System.currentTimeMillis();
    LOG.info("executeQuery processing time: " + (after - before) + "msc");

    return globalPlan.getRoot().getOutputName();
  }

  private void waitForFinishQueryUnits() throws InterruptedException {
    int i;
    Float progress = null;
    boolean wait = true;
    while (wait) {
      Thread.sleep(1000);
      LOG.info("><><><><><><>< InProgressQueries: " + qm.getAllProgresses());
      for (i = 0; i < waitQueue.size(); i++) {
        // progress = qm.getProgress(step.getQuery(i).getId());
        InProgressStatus inprogress = qm.getProgress(waitQueue.get(i).getId());
        if (inprogress != null) {
          progress = inprogress.getProgress();
          if (progress != null && progress < 1.f) {
            break;
          }
        } else {
          break;
        }
      }
      if (i == waitQueue.size()) {
        wait = false;
      }
    }
  }
  
  private String getRandomHost() 
      throws KeeperException, InterruptedException {
    List<String> serverNames = master.getOnlineServer();
    return serverNames.get(rand.nextInt(serverNames.size()));
  }
  
  public Map<QueryUnitId, Float> getProgress(SubQueryId subqid) {
    Map<QueryUnitId, Float> progressMap = new HashMap<QueryUnitId, Float>();
    
    
    
    return progressMap;
  }
  
  public void updateQuery(String nql) throws NTAQueryException {

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
