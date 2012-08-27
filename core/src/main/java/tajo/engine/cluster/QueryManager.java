/**
 * 
 */
package tajo.engine.cluster;

import com.google.common.collect.MapMaker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.engine.MasterInterfaceProtos.InProgressStatusProto;
import tajo.engine.*;
import tajo.engine.exception.NoSuchQueryIdException;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.global.QueryUnitAttempt;
import tajo.engine.planner.global.ScheduleUnit;
import tajo.engine.query.TQueryUtil;

import java.util.*;

/**
 *
 * @author jihoon
 *
 */
public class QueryManager {
  private final Log LOG = LogFactory.getLog(QueryManager.class);

  private Map<QueryId, Query> queries;
  private Map<QueryUnit, String> serverByQueryUnit;
  private Map<String, List<QueryUnit>> queryUnitsByServer;

  public QueryManager() {
    MapMaker mapMaker = new MapMaker().concurrencyLevel(4);
    
    queries = mapMaker.makeMap();
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
