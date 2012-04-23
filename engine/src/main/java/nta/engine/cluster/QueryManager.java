/**
 * 
 */
package nta.engine.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nta.catalog.statistics.Stat;
import nta.catalog.statistics.StatSet;
import nta.catalog.statistics.TableStat;
import nta.engine.LogicalQueryUnitId;
import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.TCommonProtos.StatType;
import nta.engine.Query;
import nta.engine.QueryId;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitScheduler;
import nta.engine.SubQuery;
import nta.engine.SubQueryId;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.global.QueryUnit;

import com.google.common.collect.MapMaker;

/**
 * @author jihoon
 *
 */
public class QueryManager {

  private final static long EXPIRE_TIME = 5000;
  
  public class WaitStatus {
    private InProgressStatus status;
    private long expire;
    
    public WaitStatus(InProgressStatus status, long expire) {
      set(status, expire);
    }
    
    public WaitStatus set(InProgressStatus status, long expire) {
      this.status = status;
      this.expire = expire;
      return this;
    }
    
    public WaitStatus reset() {
      this.expire = EXPIRE_TIME;
      return this;
    }
    
    public void update(long period) {
      this.expire -= period;
    }
    
    public InProgressStatus getInProgressStatus() {
      return this.status;
    }
    
    public long getLeftTime() {
      return this.expire;
    }
  }
  
  private Map<SubQuery, QueryUnitScheduler> subQueries = 
      new HashMap<SubQuery, QueryUnitScheduler>();
  private Map<QueryId, Query> queries = 
      new HashMap<QueryId, Query>();
  
  private Map<String, TableStat> statSetOfTable;
  
  private Map<QueryUnit, String> serverByQueryUnit;
  private Map<String, List<QueryUnit>> queryUnitsByServer;
  
  private Map<QueryUnitId, WaitStatus> inProgressQueries;
  
  public QueryManager() {
    MapMaker mapMaker = new MapMaker();
    serverByQueryUnit = mapMaker.concurrencyLevel(4).makeMap();
    queryUnitsByServer = mapMaker.makeMap();
    inProgressQueries = mapMaker.makeMap();
    statSetOfTable = mapMaker.makeMap();
  }
  
  public void addQuery(Query q) {
    queries.put(q.getId(), q);
  }
  
  public void addSubQuery(SubQuery subQuery) throws NoSuchQueryIdException {
    QueryId qid = subQuery.getId().getQueryId();
    if (queries.containsKey(qid)) {
      queries.get(qid).addSubQuery(subQuery);
    } else {
      throw new NoSuchQueryIdException("QueryId: " + qid);
    }
  }
  
  public void addLogicalQueryUnit(ScheduleUnit logicalQueryUnit)
  throws NoSuchQueryIdException {
    SubQueryId subId = logicalQueryUnit.getId().getSubQueryId();
    QueryId qid = subId.getQueryId();
    if (queries.containsKey(qid)) {
      SubQuery subQuery = queries.get(qid).getSubQuery(subId);
      if (subQuery != null) {
        subQuery.addLogicalQueryUnit(logicalQueryUnit);
      } else {
        throw new NoSuchQueryIdException("SubQueryId: " + subId);
      }
    } else {
      throw new NoSuchQueryIdException("QueryId: " + qid);
    }
  }
  
//  public void addQueryUnits(QueryUnit[] queryUnit) throws NoSuchQueryIdException {
//    LogicalQueryUnitId logicalId = queryUnit[0].getId().getLogicalQueryUnitId();
//    SubQueryId subId = logicalId.getSubQueryId();
//    QueryId qid = subId.getQueryId();
//    if (queries.containsKey(qid)) {
//      SubQuery subQuery = queries.get(qid).getSubQuery(subId);
//      if (subQuery != null) {
//        LogicalQueryUnit logicalUnit = subQuery.getLogicalQueryUnit(logicalId);
//        if (logicalUnit != null) {
//          queryUnitsForLogicalQueryUnit.put(logicalUnit, queryUnit);
//        } else {
//          throw new NoSuchQueryIdException("LogicalQueryUnitId: " + logicalId);
//        }
//      } else {
//        throw new NoSuchQueryIdException("SubQueryId: " + subId);
//      }
//    } else {
//      throw new NoSuchQueryIdException("QueryId: " + qid);
//    }
//  }
  
  public synchronized void updateQueryAssignInfo(String servername, QueryUnit unit) {
    serverByQueryUnit.put(unit, servername);
    List<QueryUnit> units;
    if (queryUnitsByServer.containsKey(servername)) {
      units = queryUnitsByServer.get(servername);
    } else {
      units = new ArrayList<QueryUnit>();
    }
    units.add(unit);
    queryUnitsByServer.put(servername, units);
  }
  
  public synchronized void updateProgress(QueryUnitId queryUnitId, 
      InProgressStatus progress) {
    if (inProgressQueries.containsKey(queryUnitId)) {
      inProgressQueries.put(queryUnitId, 
          inProgressQueries.get(queryUnitId).set(progress, EXPIRE_TIME));
    } else {
      inProgressQueries.put(queryUnitId, new WaitStatus(progress, EXPIRE_TIME));
    }
  }
  
  public void addQueryUnitScheduler(SubQuery subQuery, QueryUnitScheduler scheduler) {
    subQueries.put(subQuery, scheduler);
  }
  
  public Query getQuery(QueryId queryId) {
    return this.queries.get(queryId);
  }
  
  public Iterator<Query> getQueryIterator() {
    return this.queries.values().iterator();
  }
  
  public SubQuery getSubQuery(SubQueryId subQueryId) {
    Query query = queries.get(subQueryId.getQueryId());
    return query.getSubQuery(subQueryId);
  }
  
  public ScheduleUnit getLogicalQueryUnit(LogicalQueryUnitId logicalUnitId) {
    SubQueryId subId = logicalUnitId.getSubQueryId();
    return getSubQuery(subId).getLogicalQueryUnit(logicalUnitId);
  }
  
  public WaitStatus getWaitStatus(QueryUnitId unitId) {
    return this.inProgressQueries.get(unitId);
  }
  
  public InProgressStatus getProgress(QueryUnitId queryUnitId) {
    if (inProgressQueries.containsKey(queryUnitId)) {
      return this.inProgressQueries.get(queryUnitId).status;
    } else {
      return null;
    }
  }
  
  public Map<QueryUnitId, InProgressStatus> getAllProgresses() {
    Map<QueryUnitId, InProgressStatus> map = 
        new HashMap<QueryUnitId, InProgressStatus>();
    Iterator<Entry<QueryUnitId, WaitStatus>> it = inProgressQueries.entrySet().iterator();
    while (it.hasNext()) {
      Entry<QueryUnitId, WaitStatus> e = it.next();
      map.put(e.getKey(), e.getValue().status);
    }
    return map;
  }
  
  public QueryUnit[] getQueryUnitsExecutedByWorker(String serverName) {
    List<QueryUnit> units = queryUnitsByServer.get(serverName);
    return units.toArray(new QueryUnit[units.size()]);
  }
  
  public List<String> getAssignedWorkers(Query query) {
    Iterator<SubQuery> it = query.getSubQueryIterator();
    List<String> servernames = new ArrayList<String>();
    while (it.hasNext()) {
      servernames.addAll(getAssignedWorkers(it.next()));
    }
    return servernames;
  }
  
  public List<String> getAssignedWorkers(SubQuery subQuery) {
    Iterator<ScheduleUnit> it = subQuery.getLogicalQueryUnitIterator();
    List<String> servernames = new ArrayList<String>();
    while (it.hasNext()) {
      servernames.addAll(getAssignedWorkers(it.next()));
    }
    return servernames;
  }
  
  public String getAssignedWorker(QueryUnit unit) {
    return serverByQueryUnit.get(unit);
  }
  
  public WaitStatus[] getWaitStatusOfLogicalUnit(LogicalQueryUnitId id) {
    ScheduleUnit logicalUnit = getLogicalQueryUnit(id);
    return getWaitStatusOfLogicalUnit(logicalUnit);
  }
  
  public WaitStatus[] getWaitStatusOfLogicalUnit(ScheduleUnit logicalUnit) {
    QueryUnit[] units = logicalUnit.getQueryUnits();
    WaitStatus[] statuses = new WaitStatus[units.length];
    for (int i = 0; i < units.length; i++) {
      statuses[i] = inProgressQueries.get(units[i].getId());
    }
    return statuses;
  }
  
  public boolean isFinished(LogicalQueryUnitId id) {
    ScheduleUnit logicalUnit = getLogicalQueryUnit(id);
    QueryUnit[] units = logicalUnit.getQueryUnits();
    WaitStatus status;
    for (QueryUnit unit : units) {
      status = inProgressQueries.get(unit.getId());
      if (status != null) {
        if (status.getInProgressStatus().getStatus() != QueryStatus.FINISHED) {
          return false;
        }
      } else {
        return false;
      }
    }
    statSetOfTable.put(getLogicalQueryUnit(id).getOutputName(), 
        mergeStatSet(units));
    return true;
  }
  
  public TableStat getStatSet(String tableId) {
    return statSetOfTable.get(tableId);
  }
  
  private TableStat mergeStatSet(QueryUnit[] units) {
    WaitStatus status;
    TableStat tableStat = new TableStat();
    for (QueryUnit unit : units) {
      status = inProgressQueries.get(unit.getId());
      StatSet statSet = new StatSet(status.getInProgressStatus().getStats());
      for (Stat stat : statSet.getAllStats()) {
        switch (stat.getType()) {
        case COLUMN_NUM_NULLS:
          // TODO
          break;
        case TABLE_AVG_ROWS:
          if (tableStat.getAvgRows() == null) {
            tableStat.setAvgRows(stat.getValue());
          } else {
            tableStat.setAvgRows(tableStat.getAvgRows()+stat.getValue());
          }
          break;
        case TABLE_NUM_BLOCKS:
          if (tableStat.getNumBlocks() == null) {
            tableStat.setNumBlocks((int)stat.getValue());
          } else {
            tableStat.setNumBlocks(tableStat.getNumBlocks()+
                (int)stat.getValue());
          }
          break;
        case TABLE_NUM_BYTES:
          if (tableStat.getNumBytes() == null) {
            tableStat.setNumBytes(stat.getValue());
          } else {
            tableStat.setNumBytes(tableStat.getNumBytes()+stat.getValue());
          }
          break;
        case TABLE_NUM_PARTITIONS:
          if (tableStat.getNumPartitions() == null) {
            tableStat.setNumPartitions((int)stat.getValue());
          } else {
            tableStat.setNumPartitions(tableStat.getNumPartitions()+
                (int)stat.getValue());
          }
          break;
        case TABLE_NUM_ROWS:
          if (tableStat.getNumRows() == null) {
            tableStat.setNumRows(stat.getValue());
          } else {
            tableStat.setNumRows(tableStat.getNumRows()+stat.getValue());
          }
          break;
        }
      }
    }
    return tableStat;
  }
  
  public List<String> getAssignedWorkers(ScheduleUnit unit) {
    QueryUnit[] queryUnits = unit.getQueryUnits();
    if (queryUnits == null) {
      System.out.println(">>>>>> " + unit.getId());
    }
    List<String> servernames = new ArrayList<String>();
    for (QueryUnit q : queryUnits) {
      servernames.add(getAssignedWorker(q));
    }
    return servernames;
  }
}
