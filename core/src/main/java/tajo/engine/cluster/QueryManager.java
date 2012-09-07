/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package tajo.engine.cluster;

import com.google.common.collect.MapMaker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.*;
import tajo.engine.MasterWorkerProtos.InProgressStatusProto;
import tajo.engine.exception.NoSuchQueryIdException;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.global.QueryUnitAttempt;
import tajo.master.SubQuery;
import tajo.master.Query;
import tajo.util.TQueryUtil;

import java.util.*;

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
  
  public QueryUnit getQueryUnit(QueryUnitId queryUnitId) {
    return getSubQuery(queryUnitId.getSubQueryId()).
        getQueryUnit(queryUnitId);
  }

  public QueryUnitAttempt getQueryUnitAttempt(QueryUnitAttemptId attemptId) {
    return getQueryUnit(attemptId.getQueryUnitId()).getAttempt(attemptId);
  }

  public Collection<InProgressStatusProto> getAllProgresses() {
    Collection<InProgressStatusProto> statuses = new ArrayList<InProgressStatusProto>();
    for (Query query : queries.values()) {
      for (SubQuery subQuery : query.getSubQueries()) {
        for (QueryUnit queryUnit : subQuery.getQueryUnits()) {
          statuses.add(TQueryUtil.getInProgressStatusProto(queryUnit));
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
  
  public String getAssignedWorker(QueryUnit unit) {
    return serverByQueryUnit.get(unit);
  }
  
  public List<String> getAssignedWorkers(SubQuery unit) {
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
