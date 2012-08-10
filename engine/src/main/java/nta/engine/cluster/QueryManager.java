/**
 * 
 */
package nta.engine.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import nta.catalog.statistics.TableStat;
import nta.engine.*;
import nta.engine.MasterInterfaceProtos.*;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.QueryUnitAttempt;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.query.InProgressStatus;
import nta.engine.query.TQueryUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.MapMaker;

/**
 *
 * @author jihoon
 *
 */
public class QueryManager {
  private final Log LOG = LogFactory.getLog(QueryManager.class);

  private Map<QueryId, Query> queries;
  private Map<QueryId, QueryStatus> queryStatusMap;
  private Map<SubQueryId, QueryStatus> subQueryStatusMap;
  private Map<ScheduleUnitId, QueryStatus> scheduleUnitStatusMap;
  private Map<QueryUnitId, QueryStatus> queryUnitStatusMap;
  
  private Map<QueryUnit, String> serverByQueryUnit;
  private Map<String, List<QueryUnit>> queryUnitsByServer;

  public QueryManager() {
    MapMaker mapMaker = new MapMaker().concurrencyLevel(4);
    
    queries = mapMaker.makeMap();
    queryStatusMap = mapMaker.makeMap();
    subQueryStatusMap = mapMaker.makeMap();
    scheduleUnitStatusMap = mapMaker.makeMap();
    queryUnitStatusMap = mapMaker.makeMap();
    serverByQueryUnit = mapMaker.makeMap();
    queryUnitsByServer = mapMaker.makeMap();
  }

  public Query getQuery(String queryStr) {
    for (Query query : queries.values()) {
      if (query.getQueryStr().equals(queryStr)) {
        return query;
      }
    }
    return null;
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
  
  public void addScheduleUnit(ScheduleUnit scheduleUnit)
  throws NoSuchQueryIdException {
    ScheduleUnitId scheduleId = scheduleUnit.getId();
    Query query = queries.get(scheduleId.getQueryId());
    if (query != null) {
      SubQuery subQuery = query.getSubQuery(scheduleId.getSubQueryId());
      if (subQuery != null) {
        subQuery.addScheduleUnit(scheduleUnit);
      }
      else {
        throw new NoSuchQueryIdException("SubQueryId: " + 
            scheduleId.getSubQueryId());
      }
    } else {
      throw new NoSuchQueryIdException("QueryId: " + 
          scheduleId.getQueryId());
    }
  }

  public void updateSubQueryStatus(SubQueryId subQueryId,
                                   QueryStatus status) {
    subQueryStatusMap.put(subQueryId, status);
    LOG.info("SubQuery status of " + subQueryId +
        " is changed to " + status);
  }

  public synchronized void updateScheduleUnitStatus(
      final ScheduleUnitId scheduleUnitId,
      final QueryStatus status) {
    scheduleUnitStatusMap.put(scheduleUnitId, status);
    LOG.info("ScheduleUnit status of " + scheduleUnitId +
        " is changed to " + status);
  }

  public synchronized void updateQueryUnitStatus(final QueryUnitId queryUnitId,
                                    final QueryStatus status) {
    queryUnitStatusMap.put(queryUnitId, status);
    LOG.info("QueryUnit status of " + queryUnitId +
        " is changed to " + status);
  }

  public QueryStatus getSubQueryStatus(SubQuery subQuery) {
    return this.getSubQueryStatus(subQuery.getId());
  }

  public QueryStatus getSubQueryStatus(SubQueryId subQueryId) {
    return subQueryStatusMap.get(subQueryId);
  }

  public QueryStatus getScheduleUnitStatus(ScheduleUnit scheduleUnit) {
    return this.getScheduleUnitStatus(scheduleUnit.getId());
  }

  public QueryStatus getScheduleUnitStatus(ScheduleUnitId scheduleUnitId) {
    return scheduleUnitStatusMap.get(scheduleUnitId);
  }

  public QueryStatus getQueryUnitStatus(QueryUnitId queryUnitId) {
    return queryUnitStatusMap.get(queryUnitId);
  }

  public void updateQueryStatus(Query query, QueryStatus status) {
    updateQueryStatus(query.getId(), status);
  }

  public synchronized void updateQueryStatus(QueryId queryId,
         QueryStatus status) {
    queryStatusMap.put(queryId, status);
    LOG.info("Query status of " + queryId +
        " is changed to " + status);
  }

  public QueryStatus getQueryStatus(Query query) {
    return getQueryStatus(query.getId());
  }

  public QueryStatus getQueryStatus(QueryId queryId) {
    return queryStatusMap.get(queryId);
  }
  
  public void updateQueryAssignInfo(String servername,
      QueryUnit unit) {
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

  /**
   *
   * @param attemptId
   * @param progress
   * @throws NoSuchQueryIdException
   */
  @Deprecated
  public void updateProgress(QueryUnitAttemptId attemptId,
      InProgressStatusProto progress) throws NoSuchQueryIdException {
    QueryUnitAttempt unit = queries.get(attemptId.getQueryId()).
        getQueryUnit(attemptId.getQueryUnitId()).getLastAttempt();
    if (unit != null) {
      unit.setProgress(progress.getProgress());
      unit.setStatus(progress.getStatus());
      if (progress.getPartitionsCount() > 0) {
        unit.getQueryUnit().setPartitions(progress.getPartitionsList());
      }
      if (progress.hasResultStats()) {
        unit.getQueryUnit().setStats(new TableStat(progress.getResultStats()));
      }
      unit.resetExpireTime();
    } else {
      throw new NoSuchQueryIdException("QueryUnitAttemptId: " + attemptId);
    }
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
  
  public ScheduleUnit getScheduleUnit(ScheduleUnitId scheduleUnitId) {
    SubQueryId subId = scheduleUnitId.getSubQueryId();
    return getSubQuery(subId).getScheduleUnit(scheduleUnitId);
  }
  
  public QueryUnit getQueryUnit(QueryUnitId queryUnitId) {
    return getScheduleUnit(queryUnitId.getScheduleUnitId()).
        getQueryUnit(queryUnitId);
  }

  public QueryUnitAttempt getQueryUnitAttempt(QueryUnitAttemptId attemptId) {
    return getQueryUnit(attemptId.getQueryUnitId()).getAttempt(attemptId);
  }

  public Collection<InProgressStatusProto> getAllProgresses() {
    Collection<InProgressStatusProto> statuses = new ArrayList<InProgressStatusProto>();
    for (Query query : queries.values()) {
      for (SubQuery subQuery : query.getSubQueries()) {
        for (ScheduleUnit scheduleUnit : subQuery.getScheduleUnits()) {
          for (QueryUnit queryUnit : scheduleUnit.getQueryUnits()) {
            statuses.add(TQueryUtil.getInProgressStatusProto(queryUnit));
          }
        }
      }
    }
    return statuses;
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
    Iterator<ScheduleUnit> it = subQuery.getScheduleUnitIterator();
    List<String> servernames = new ArrayList<String>();
    while (it.hasNext()) {
      servernames.addAll(getAssignedWorkers(it.next()));
    }
    return servernames;
  }
  
  public String getAssignedWorker(QueryUnit unit) {
    return serverByQueryUnit.get(unit);
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
