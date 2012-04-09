/**
 * 
 */
package nta.engine;

import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import nta.catalog.TCatUtil;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.cluster.ClusterManager;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.QueryManager.WaitStatus;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.global.ScheduleUnit.PARTITION_TYPE;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.logical.ScanNode;
import nta.engine.query.GlobalQueryPlanner;
import nta.engine.query.QueryUnitRequestImpl;
import nta.storage.StorageManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author jihoon
 *
 */
public class QueryUnitScheduler extends Thread {
  
  private final static long WAIT_PERIOD = 1000;
  
  private Log LOG = LogFactory.getLog(QueryUnitScheduler.class);
  
  private final StorageManager sm;
  private final WorkerCommunicator wc;
  private final GlobalQueryPlanner planner;
  private final ClusterManager cm;
  private final QueryManager qm;
  private final ScheduleUnit plan;
  
  private BlockingQueue<QueryUnit> pendingQueue = 
      new LinkedBlockingQueue<QueryUnit>();
  private BlockingQueue<QueryUnit> waitQueue = 
      new LinkedBlockingQueue<QueryUnit>();
  
  public QueryUnitScheduler(Configuration conf, StorageManager sm, 
      ClusterManager cm, QueryManager qm, WorkerCommunicator wc, 
      GlobalQueryPlanner planner, ScheduleUnit plan) {
    this.sm = sm;
    this.cm = cm;
    this.qm = qm;
    this.wc = wc;
    this.planner = planner;
    this.plan = plan;
  }
  
  private void recursiveExecuteQueryUnit(ScheduleUnit plan) 
      throws Exception {
    if (plan.hasPrevQuery()) {
      Iterator<ScheduleUnit> it = plan.getPrevIterator();
      while (it.hasNext()) {
        recursiveExecuteQueryUnit(it.next());
      }
    }
    
    qm.addLogicalQueryUnit(plan);
    LOG.info("Table path " + sm.getTablePath(plan.getOutputName()).toString()
        + " is initialized for " + plan.getOutputName());
    if (plan.getOutputType() == PARTITION_TYPE.HASH) {
      Path tablePath = sm.getTablePath(plan.getOutputName());
      sm.getFileSystem().mkdirs(tablePath);
    } else {
      sm.initTableBase(TCatUtil.newTableMeta(plan.getOutputSchema(), StoreType.CSV), 
          plan.getOutputName());
    }
    
    // TODO: adjust the number of localization
    QueryUnit[] units = planner.localize(plan, cm.getOnlineWorker().size());
    String hostName;
    for (QueryUnit q : units) {
      hostName = cm.getProperHost(q);
      if (hostName == null) {
        hostName = cm.getRandomHost();
      }
      q.setHost(hostName);
      pendingQueue.add(q);
      qm.updateQueryAssignInfo(hostName, q);
    }
    requestPendingQueryUnits();
    
    waitForFinishQueryUnits();
    waitQueue.clear();
  }
  
  private void requestPendingQueryUnits() throws Exception {
    while (!pendingQueue.isEmpty()) {
      QueryUnit q = pendingQueue.take();
      waitQueue.add(q);
      QueryUnitRequest request = new QueryUnitRequestImpl(q.getId(), q.getFragments(), 
          q.getOutputName(), false, q.getLogicalPlan().toJSON());
      
      if (q.getStoreTableNode().isLocal()) {
        request.setInterQuery();
      }
      ScanNode [] scans = q.getScanNodes();
      for (ScanNode scan : scans) {
        if (scan.isLocal()) {
          Collection<List<URI>> fetchHosts =  q.getFetches();
          for (List<URI> hosts : fetchHosts) {
            for (URI host : hosts) {
              request.addFetch(scan.getTableId(), host);
            }
          }
        }
      }
      
      wc.requestQueryUnit(q.getHost(), request.getProto());
      LOG.info("QueryUnitRequest " + q.getId() + " is sent to " + (q.getHost()));
      LOG.info("QueryStep's output name " + q.getStoreTableNode().getTableName());
    }
  }
  
  private void waitForFinishQueryUnits() throws Exception {
    boolean wait = true;
    while (wait) {
      Thread.sleep(WAIT_PERIOD);
      wait = false;
      Iterator<QueryUnit> it = waitQueue.iterator();
      while (it.hasNext()) {
        QueryUnit unit = it.next();
        WaitStatus inprogress = qm.getWaitStatus(unit.getId());
        if (inprogress != null) {
          LOG.info("==== uid: " + unit.getId() + 
              " status: " + inprogress.getInProgressStatus() + 
              " left time: " + inprogress.getLeftTime());
          if (inprogress.getInProgressStatus().
              getStatus() != QueryStatus.FINISHED) {
            inprogress.update(WAIT_PERIOD);
            wait = true;
            if (inprogress.getLeftTime() <= 0) {
              waitQueue.remove(unit);
              requestBackupTask(unit);
              inprogress.reset();
            }
          }
        } else {
          wait = true;
        }
      }
    }
  }
  
  private void requestBackupTask(QueryUnit q) throws Exception {
    FileSystem fs = sm.getFileSystem();
    Path path = new Path(sm.getTablePath(q.getOutputName()), 
        q.getId().toString());
    fs.delete(path, true);
    String hostName = cm.getProperHost(q);
    if (hostName == null) {
      hostName = cm.getRandomHost();
    }
    q.setHost(hostName);
    LOG.info("QueryUnit " + q.getId() + " is assigned to " + 
        q.getHost() + " as the backup task");
    pendingQueue.add(q);
    requestPendingQueryUnits();
  }
  
  @Override
  public void run() {
    try {
      long before = System.currentTimeMillis();
      recursiveExecuteQueryUnit(this.plan);
      long after = System.currentTimeMillis();
      LOG.info("executeQuery processing time: " + (after - before) + "msc");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
}
