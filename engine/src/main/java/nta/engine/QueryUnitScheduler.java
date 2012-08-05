/**
 * 
 */
package nta.engine;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.statistics.ColumnStat;
import nta.catalog.statistics.StatisticsUtil;
import nta.catalog.statistics.TableStat;
import nta.engine.MasterInterfaceProtos.*;
import nta.engine.cluster.*;
import nta.engine.exception.EmptyClusterException;
import nta.engine.exception.UnknownWorkerException;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.query.GlobalPlanner;
import nta.engine.query.GlobalPlannerUtils;
import nta.engine.query.QueryUnitRequestImpl;
import nta.storage.StorageManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author jihoon
 *
 */
public class QueryUnitScheduler extends Thread {
  
  private final static int WAIT_PERIOD = 3000;
  private final static int RETRY_LIMIT = 3;
  
  private Log LOG = LogFactory.getLog(QueryUnitScheduler.class);
  
  private final StorageManager sm;
  private final WorkerCommunicator wc;
  private final GlobalPlanner planner;
  private final ClusterManager cm;
  private final QueryManager qm;
  private final ScheduleUnit plan;
  private List<String> onlineWorkers;

  private BlockingQueue<QueryUnit> pendingQueue = 
      new LinkedBlockingQueue<QueryUnit>();
  private Map<QueryUnitId, Integer> queryUnitAttemptMap;

  public QueryUnitScheduler(Configuration conf, StorageManager sm, 
      ClusterManager cm, QueryManager qm, WorkerCommunicator wc, 
      GlobalPlanner planner, ScheduleUnit plan) throws EmptyClusterException {
    this.sm = sm;
    this.cm = cm;
    this.qm = qm;
    this.wc = wc;
    this.planner = planner;
    this.plan = plan;
    onlineWorkers = Lists.newArrayList();
    updateWorkers();
    queryUnitAttemptMap = Maps.newHashMap();
  }

  private void updateWorkers() throws EmptyClusterException {
    cm.updateOnlineWorker();
    onlineWorkers.clear();
    for (List<String> workers : cm.getOnlineWorkers().values()) {
      onlineWorkers.addAll(workers);
    }
//    onlineWorkers.removeAll(cm.getFailedWorkers());
    if (onlineWorkers.size() == 0) {
      throw new EmptyClusterException();
    }
  }

  private void recursiveExecuteScheduleUnit(ScheduleUnit plan)
      throws Exception {
    if (qm.getQueryStatus(plan.getId().getQueryId())
        != QueryStatus.QUERY_INPROGRESS ||
        qm.getSubQueryStatus(plan.getId().getSubQueryId())
            != QueryStatus.QUERY_INPROGRESS) {
      return;
    }

    if (plan.hasChildQuery()) {
      Iterator<ScheduleUnit> it = plan.getChildIterator();
      while (it.hasNext()) {
        ScheduleUnit su = it.next();
        recursiveExecuteScheduleUnit(su);
      }
    }
    
    if (plan.getStoreTableNode().getSubNode().getType() == ExprType.UNION) {
      // union operators are not executed
    } else {
      LOG.info("Plan of " + plan.getId() + " : " + plan.getLogicalPlan());
      qm.addScheduleUnit(plan);

      qm.updateScheduleUnitStatus(plan.getId(),
          QueryStatus.QUERY_INITED);
      qm.updateScheduleUnitStatus(plan.getId(),
          QueryStatus.QUERY_PENDING);
      qm.updateScheduleUnitStatus(plan.getId(),
          QueryStatus.QUERY_INPROGRESS);

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
        plan.setStats(stat);
        //plan.setStatus(QueryStatus.QUERY_FINISHED);
        qm.updateScheduleUnitStatus(plan.getId(),
            QueryStatus.QUERY_FINISHED);
        return;
      } else {
        units = prepareExecutionOfQueryUnits(units, plan.hasChildQuery());

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
        if (qm.getScheduleUnitStatus(plan) == QueryStatus.QUERY_FINISHED) {
          TableMeta meta = TCatUtil.newTableMeta(plan.getOutputSchema(),
              StoreType.CSV);
          meta.setStat(stat);
          sm.writeTableMeta(sm.getTablePath(plan.getOutputName()), meta);
          qm.getSubQuery(units[0].getId().getSubQueryId()).setTableStat(stat);
        } else {
          LOG.error("ScheduleUnit " + plan.getId() +
              " is terminated with " + qm.getScheduleUnitStatus(plan));
        }
      }
    }
  }

  private QueryUnit[] prepareExecutionOfQueryUnits(QueryUnit[] units,
                                                   boolean hasChild)
      throws Exception {
    if (hasChild) {
      for (QueryUnit q : units) {
        q.setHost(cm.getRandomHost());
      }
    } else {
      Map<Fragment, FragmentServingInfo> servingMap =
          Maps.newHashMap();
      for (QueryUnit q : units) {
        for (Fragment f : q.getAllFragments()) {
          servingMap.put(f, cm.getServingInfoMap().get(f));
        }
      }
      units = GlobalPlannerUtils.buildQueryDistributionPlan(
          servingMap, cm.getOnlineWorkers(), /*cm.getFailedWorkers(), */
          units);
    }
    for (QueryUnit q : units) {
      pendingQueue.add(q);
      qm.updateQueryAssignInfo(q.getHost(), q);
      this.queryUnitAttemptMap.put(q.getId(), 1);
    }
    return units;
  }

  private void requestPendingQueryUnits() throws Exception {
    while (!pendingQueue.isEmpty()) {
      QueryUnit q = pendingQueue.take();
      List<Fragment> fragList = new ArrayList<Fragment>();
      for (ScanNode scan : q.getScanNodes()) {
        fragList.add(q.getFragment(scan.getTableId()));
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

      if (!requestToWC(q.getHost(), request.getProto())) {
        retryQueryUnit(q);
      }
      LOG.info("=====================================================================");
      LOG.info("QueryUnitRequest " + request.getId() + " is sent to " + (q.getHost()));
      LOG.info("QueryStep's output name " + q.getStoreTableNode().getTableName());
      if (request.isInterQuery()) {
        LOG.info("InterQuery is enabled");
      } else {
        LOG.info("InterQuery is disabled");
      }
      LOG.info("=====================================================================");
    }
  }
  
  private TableStat waitForFinishScheduleUnit(ScheduleUnit scheduleUnit) 
      throws Exception {
    boolean wait = true;
    boolean retryQueryUnit = false;
    QueryUnit[] units = scheduleUnit.getQueryUnits();
    QueryStatus status;
    QueryStatus finalScheduleUnitStatus = null;
    Command.Builder cmd = null;

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
        retryQueryUnit = false;
        /*status = unit.getInProgressStatus().getStatus();
        qm.updateQueryUnitStatus(unit.getId(),
            queryUnitAttemptMap.get(unit.getId()), status);*/
        status = qm.getQueryUnitStatus(unit.getId()).getLastAttempt().getStatus();

        switch (status) {
          case QUERY_KILLED:
            sendCommand(unit, CommandType.STOP);
            break;
          case QUERY_ABORTED:
            LOG.info("QueryUnit " + unit.getId() + " is aborted!!");
            wait = true;
            retryQueryUnit = true;
            break;
          case QUERY_FINISHED:
//          // TODO: Aggregate commands and send together
/*
            cmd = Command.newBuilder();
            cmd.setId(unit.getId().getProto()).setType(CommandType.FINALIZE);
            requestToWC(unit.getHost(), CommandRequestProto.newBuilder().
                addCommand(cmd.build()).build());
*/
            break;
          default:
            unit.updateExpireTime(WAIT_PERIOD);
            wait = true;
            if (unit.getLeftTime() <= 0) {
              LOG.info("QueryUnit " + unit.getId() + " is expired!!");
              retryQueryUnit = true;
            }
            break;
        }

        if (retryQueryUnit) {
          if (!retryQueryUnit(unit)) {
            LOG.info("The query " + scheduleUnit.getId() +
                " will be aborted, because the query unit " + unit.getId() +
                "'s status is " + status);
            wait = false;
            break;
          }
        }
      }

      for (QueryUnit unit : scheduleUnit.getQueryUnits()) {
        switch (qm.getQueryUnitStatus(unit.getId()).
            getAttempt(queryUnitAttemptMap.get(unit.getId())).getStatus()) {
          case QUERY_INITED: inited++; break;
          case QUERY_PENDING: pending++; break;
          case QUERY_INPROGRESS: inprogress++; break;
          case QUERY_FINISHED: success++; break;
          case QUERY_ABORTED: aborted++; break;
          case QUERY_KILLED: killed++; break;
        }
      }

      LOG.info("Job " + scheduleUnit.getId() + " In Progress (Total: " + units.length
          + ", Finished: " + success + ", Inited: " + inited + ", Pending: " + pending
          + ", Running: " + inprogress + ", Aborted: " + aborted + ", Killed: " + killed);
      if (units.length == success) {
        finalScheduleUnitStatus = QueryStatus.QUERY_FINISHED;
      } else {
        finalScheduleUnitStatus = QueryStatus.QUERY_ABORTED;
      }
    }
    qm.updateScheduleUnitStatus(scheduleUnit.getId(), finalScheduleUnitStatus);
    if (finalScheduleUnitStatus == QueryStatus.QUERY_FINISHED) {
      if (scheduleUnit.hasChildQuery()) {
        finalizePrevScheduleUnit(scheduleUnit);
      }
      return generateStat(scheduleUnit);
    } else {
      return null;
    }
  }

  private void sendCommand(QueryUnit unit, CommandType type)
      throws Exception {
    Command.Builder cmd = Command.newBuilder();
    cmd.setId(unit.getId().getProto()).setType(type);
    requestToWC(unit.getHost(), CommandRequestProto.newBuilder().
        addCommand(cmd.build()).build());
  }

  private void finalizePrevScheduleUnit(ScheduleUnit scheduleUnit)
      throws Exception {
    ScheduleUnit prevScheduleUnit;
    for (ScanNode scan : scheduleUnit.getScanNodes()) {
      prevScheduleUnit = scheduleUnit.getChildQuery(scan);
      if (prevScheduleUnit.getStoreTableNode().getSubNode().getType()
          != ExprType.UNION) {
        for (QueryUnit unit : prevScheduleUnit.getQueryUnits()) {
          sendCommand(unit, CommandType.FINALIZE);
        }
      }
    }
  }

  private TableStat generateStat(ScheduleUnit scheduleUnit) {
    List<TableStat> stats = Lists.newArrayList();
    for (QueryUnit unit : scheduleUnit.getQueryUnits() ) {
      stats.add(unit.getStats());
    }
    TableStat tableStat = StatisticsUtil.aggregate(stats);
    return tableStat;
  }

  private boolean requestToWC(String host, Message proto) throws Exception {
    boolean result = true;
    try {
      if (proto instanceof QueryUnitRequestProto) {
        wc.requestQueryUnit(host, (QueryUnitRequestProto)proto);
      } else if (proto instanceof CommandRequestProto) {
        wc.requestCommand(host, (CommandRequestProto)proto);
      }
    } catch (UnknownWorkerException e) {
      handleUnknownWorkerException(e);
      result = false;
    }
    return result;
  }

  private boolean retryQueryUnit(QueryUnit unit) throws Exception {
    int retryCnt = 0;
    if (queryUnitAttemptMap.containsKey(unit.getId())) {
      retryCnt = queryUnitAttemptMap.get(unit.getId());
    } else {
      LOG.error("Unregistered query unit: " + unit.getId());
      return false;
    }

    if (retryCnt < RETRY_LIMIT) {
      qm.updateQueryUnitStatus(unit.getId(), retryCnt,
          QueryStatus.QUERY_ABORTED);
      queryUnitAttemptMap.put(unit.getId(), ++retryCnt);
      commitBackupTask(unit);
      qm.updateQueryUnitStatus(unit.getId(), retryCnt,
          QueryStatus.QUERY_SUBMITED);
      return true;
    } else {
      // Cancel the executed query
      return false;
    }
  }

  private void commitBackupTask(QueryUnit unit) throws Exception {
    sendCommand(unit, CommandType.STOP);
    requestBackupTask(unit);
    unit.resetExpireTime();
  }

  private void handleUnknownWorkerException(UnknownWorkerException e) {
    LOG.info(e);
    cm.addFailedWorker(e.getUnknownName());
    LOG.info(e.getUnknownName() + " is excluded from the query planning.");
  }
  
  private void requestBackupTask(QueryUnit q) throws Exception {
    FileSystem fs = sm.getFileSystem();
    Path path = new Path(sm.getTablePath(q.getOutputName()), 
        q.getId().toString());
    fs.delete(path, true);
    cm.addFailedWorker(q.getHost());
    updateWorkers();
    String hostName = cm.getRandomHost();
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
      recursiveExecuteScheduleUnit(this.plan);
      long after = System.currentTimeMillis();
      LOG.info("executeQuery processing time: " + (after - before) + "msc");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
