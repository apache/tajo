/**
 * 
 */
package nta.engine;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.statistics.TableStat;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.cluster.ClusterManager;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.global.ScheduleUnit.PARTITION_TYPE;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.ScanNode;
import nta.engine.query.GlobalPlanner;
import nta.engine.query.QueryUnitRequestImpl;
import nta.engine.query.TQueryUtil;
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
  
  private final static int WAIT_PERIOD = 1000;
  
  private Log LOG = LogFactory.getLog(QueryUnitScheduler.class);
  
  private final StorageManager sm;
  private final WorkerCommunicator wc;
  private final GlobalPlanner planner;
  private final ClusterManager cm;
  private final QueryManager qm;
  private final ScheduleUnit plan;
  
  private BlockingQueue<QueryUnit> pendingQueue = 
      new LinkedBlockingQueue<QueryUnit>();
  
  public QueryUnitScheduler(Configuration conf, StorageManager sm, 
      ClusterManager cm, QueryManager qm, WorkerCommunicator wc, 
      GlobalPlanner planner, ScheduleUnit plan) {
    this.sm = sm;
    this.cm = cm;
    this.qm = qm;
    this.wc = wc;
    this.planner = planner;
    this.plan = plan;
  }
  
  private void recursiveExecuteQueryUnit(ScheduleUnit plan) 
      throws Exception {
    if (plan.hasChildQuery()) {
      Iterator<ScheduleUnit> it = plan.getChildIterator();
      while (it.hasNext()) {
        recursiveExecuteQueryUnit(it.next());
      }
    }
    
    if (plan.getStoreTableNode().getSubNode().getType() == ExprType.UNION) {
      // union operators are not executed
    } else {
      LOG.info("Plan of " + plan.getId() + " : " + plan.getLogicalPlan());
      qm.addScheduleUnit(plan);
      if (plan.getOutputType() == PARTITION_TYPE.HASH) {
        Path tablePath = sm.getTablePath(plan.getOutputName());
        sm.getFileSystem().mkdirs(tablePath);
        LOG.info("Table path " + sm.getTablePath(plan.getOutputName()).toString()
            + " is initialized for " + plan.getOutputName());
      } else {
        if (!sm.getFileSystem().exists(sm.getTablePath(plan.getOutputName()))) {
          sm.initTableBase(null, plan.getOutputName());
          LOG.info("Table path " + sm.getTablePath(plan.getOutputName()).toString()
              + " is initialized for " + plan.getOutputName());
        }
      }

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
      
      TableStat stat = waitForFinishScheduleUnit(plan);
      TableMeta meta = TCatUtil.newTableMeta(plan.getOutputSchema(), 
          StoreType.CSV);
      meta.setStat(stat);
      sm.writeTableMeta(sm.getTablePath(plan.getOutputName()), meta);
    }
  }
  
  private void requestPendingQueryUnits() throws Exception {
    while (!pendingQueue.isEmpty()) {
      QueryUnit q = pendingQueue.take();
      List<Fragment> fragList = new ArrayList<Fragment>();
      for (ScanNode scan : q.getScanNodes()) {
        fragList.addAll(q.getFragments(scan.getTableId()));
      }
      QueryUnitRequest request = new QueryUnitRequestImpl(q.getId(), fragList, 
          q.getOutputName(), false, q.getLogicalPlan().toJSON());
      
      if (q.getStoreTableNode().isLocal()) {
        request.setInterQuery();
      }
      
      for (ScanNode scan : q.getScanNodes()) {
        List<URI> fetches = q.getFetch(scan);
        if (fetches != null) {
          for (URI fetch : fetches) {
            request.addFetch(scan.getTableId(), fetch);
          }
        }
      }
      
      wc.requestQueryUnit(q.getHost(), request.getProto());
      LOG.info("=====================================================================");
      LOG.info("QueryUnitRequest " + request.getId() + " is sent to " + (q.getHost()));
      LOG.info("Fragments: " + request.getFragments());
      LOG.info("QueryStep's output name " + q.getStoreTableNode().getTableName());
      if (request.isInterQuery()) {
        LOG.info("InterQuery is enabled");
      } else {
        LOG.info("InterQuery is disabled");
      }
      LOG.info("Fetches: " + request.getFetches());
    }
  }
  
  private TableStat waitForFinishScheduleUnit(ScheduleUnit scheduleUnit) 
      throws Exception {
    boolean wait = true;
    QueryUnit[] units = scheduleUnit.getQueryUnits();
    while (wait) {
      Thread.sleep(WAIT_PERIOD);
      wait = false;
      for (QueryUnit unit : units) {
//        WaitStatus inprogress = qm.getWaitStatus(unit.getId());
        LOG.info("==== uid: " + unit.getId() + 
            " status: " + unit.getInProgressStatus() + 
            " left time: " + unit.getLeftTime());
        if (unit.getInProgressStatus().
            getStatus() != QueryStatus.FINISHED) {
          unit.updateExpireTime(WAIT_PERIOD);
          wait = true;
          if (unit.getLeftTime() <= 0) {
            requestBackupTask(unit);
            unit.resetExpireTime();
          }
        }
      }
    }
    TableStat tableStat = new TableStat();
    for (QueryUnit unit : units ) {
      tableStat = TQueryUtil.mergeStatSet(tableStat, unit.getStats());
    }
    return tableStat;
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
