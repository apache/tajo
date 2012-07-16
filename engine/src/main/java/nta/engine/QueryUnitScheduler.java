/**
 * 
 */
package nta.engine;

import com.google.common.collect.Lists;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.statistics.ColumnStat;
import nta.catalog.statistics.StatisticsUtil;
import nta.catalog.statistics.TableStat;
import nta.engine.MasterInterfaceProtos.Command;
import nta.engine.MasterInterfaceProtos.CommandRequestProto;
import nta.engine.MasterInterfaceProtos.CommandType;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.cluster.ClusterManager;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.query.GlobalPlanner;
import nta.engine.query.QueryUnitRequestImpl;
import nta.storage.StorageManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.net.URI;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
        ScheduleUnit su = it.next();
        recursiveExecuteQueryUnit(su);
      }
    }
    
    if (plan.getStoreTableNode().getSubNode().getType() == ExprType.UNION) {
      // union operators are not executed
    } else {
      LOG.info("Plan of " + plan.getId() + " : " + plan.getLogicalPlan());
      qm.addScheduleUnit(plan);

      switch (plan.getOutputType()) {
        case HASH:
          Path tablePath = sm.getTablePath(plan.getOutputName());
          sm.getFileSystem().mkdirs(tablePath);
          LOG.info("Table path " + sm.getTablePath(plan.getOutputName()).toString()
              + " is initialized for " + plan.getOutputName());
          break;
        case RANGE: // TODO - to be improved

        default:
          if (!sm.getFileSystem().exists(sm.getTablePath(plan.getOutputName()))) {
            sm.initTableBase(null, plan.getOutputName());
            LOG.info("Table path " + sm.getTablePath(plan.getOutputName()).toString()
                + " is initialized for " + plan.getOutputName());
          }
      }

      // TODO - the below code should be extracted to a separate method
      int numTasks;
      GroupbyNode grpNode = (GroupbyNode) PlannerUtil.findTopNode(plan.getLogicalPlan(), ExprType.GROUP_BY);
      if (plan.getParentQuery() == null && grpNode != null && grpNode.getGroupingColumns().length == 0) {
        numTasks = 1;
      } else {
        numTasks = cm.getOnlineWorkers().size();
      }

      QueryUnit[] units = planner.localize(plan, numTasks);
      // if there is empty input (or intermediate) data,
      // we don't need to execute this query.
      if (units.length == 0) {
        TableMeta meta = TCatUtil.newTableMeta(plan.getOutputSchema(), StoreType.CSV);
        TableStat stat = new TableStat();
        for (int i = 0; i < plan.getOutputSchema().getColumnNum(); i++) {
          stat.addColumnStat(new ColumnStat(plan.getOutputSchema().getColumn(i)));
        }
        qm.getSubQuery(plan.getId().getSubQueryId()).setTableStat(stat);
        plan.setStatus(QueryStatus.QUERY_FINISHED);
        return;
      } else {
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

        // this is for debugging
        Map<String,Integer> assigned = Maps.newTreeMap();
        for (QueryUnit qt : units) {
          if (assigned.containsKey(qt.getHost())) {
            assigned.put(qt.getHost(), assigned.get(qt.getHost()) + qt.getAllFragments().size());
          } else {
            assigned.put(qt.getHost(), qt.getAllFragments().size());
          }
        }
        LOG.info("=======================");
        LOG.info("= Assigned Info");
        for (Map.Entry<String,Integer> entry : assigned.entrySet()) {
          LOG.info("Host: " + entry.getKey() + ", # of Tasks: " + entry.getValue());
        }
        LOG.info("=======================");

        requestPendingQueryUnits();

        TableStat stat = waitForFinishScheduleUnit(plan);
        TableMeta meta = TCatUtil.newTableMeta(plan.getOutputSchema(),
            StoreType.CSV);
        meta.setStat(stat);
        sm.writeTableMeta(sm.getTablePath(plan.getOutputName()), meta);
        qm.getSubQuery(units[0].getId().getSubQueryId()).setTableStat(stat);
      }
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
        Collection<URI> fetches = q.getFetch(scan);
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
      LOG.info("=====================================================================");
    }
  }
  
  private TableStat waitForFinishScheduleUnit(ScheduleUnit scheduleUnit) 
      throws Exception {
    boolean wait = true;
    QueryUnit[] units = scheduleUnit.getQueryUnits();
    while (wait) {
      Thread.sleep(WAIT_PERIOD);
      wait = false;

      int inited = 0;
      int pending = 0;
      int inprogress = 0;
      int success = 0;
      int aborted = 0;
      int killed = 0;

      for (QueryUnit unit : units) {
/*        LOG.info("==== uid: " + unit.getId() +
            " status: " + unit.getInProgressStatus() + 
            " left time: " + unit.getLeftTime());*/
        switch (unit.getInProgressStatus().getStatus()) {
          case QUERY_INITED: inited++; break;
          case QUERY_PENDING: pending++; break;
          case QUERY_INPROGRESS: inprogress++; break;
          case QUERY_FINISHED: success++; break;
          case QUERY_ABORTED: aborted++; break;
          case QUERY_KILLED: killed++; break;
        }

        if (unit.getInProgressStatus().
            getStatus() != QueryStatus.QUERY_FINISHED) {
          unit.updateExpireTime(WAIT_PERIOD);
          wait = true;
          if (unit.getLeftTime() <= 0) {
            // TODO: Aggregate commands and send together
            // send stop
            Command.Builder cmd = Command.newBuilder();
            cmd.setId(unit.getId().getProto()).setType(CommandType.STOP);
            wc.requestCommand(unit.getHost(), 
                CommandRequestProto.newBuilder().addCommand(cmd.build()).build());
            requestBackupTask(unit);
            unit.resetExpireTime();
          }
//        } else if (unit.getInProgressStatus().
//            getStatus() == QueryStatus.FINISHED) {
//          // TODO: Aggregate commands and send together
//          Command.Builder cmd = Command.newBuilder();
//          cmd.setId(unit.getId().getProto()).setType(CommandType.FINALIZE);
//          wc.requestCommand(unit.getHost(), 
//              CommandRequestProto.newBuilder().addCommand(cmd.build()).build());
        }
      }

      LOG.info("Job " + scheduleUnit.getId() + " In Progress (Total: " + units.length
          + ", Finished: " + success + ", Inited: " + inited + ", Pending: " + pending
          + ", Running: " + inprogress + ", Aborted: " + aborted + ", Killed: " + killed);
    }
    List<TableStat> stats = Lists.newArrayList();
    for (QueryUnit unit : units ) {
      stats.add(unit.getStats());
    }
    TableStat tableStat = StatisticsUtil.aggregate(stats);
    return tableStat;
  }
  
  private void requestBackupTask(QueryUnit q) throws Exception {
    FileSystem fs = sm.getFileSystem();
    Path path = new Path(sm.getTablePath(q.getOutputName()), 
        q.getId().toString());
    fs.delete(path, true);
    String prevHost = q.getHost();
    String hostName = cm.getProperHost(q);
    if (hostName == null ||
        hostName.equals(prevHost)) {
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
