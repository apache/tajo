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

package tajo.master;

import com.google.common.collect.Maps;
import tajo.QueryId;
import tajo.QueryUnitId;
import tajo.SubQueryId;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.planner.global.QueryUnit;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class Query extends AbstractQuery {

  private final QueryId id;
  private String queryStr;
  private Map<SubQueryId, SubQuery> subqueries;
  private QueryStatus status;
  
  public Query(QueryId id, String queryStr) {
    this.id = id;
    this.queryStr = queryStr;
    subqueries = Maps.newHashMap();
  }
  
  public void addSubQuery(SubQuery q) {
    subqueries.put(q.getId(), q);
  }
  
  public QueryId getId() {
    return this.id;
  }

  public String getQueryStr() {
    return this.queryStr;
  }

  public Iterator<SubQuery> getSubQueryIterator() {
    return this.subqueries.values().iterator();
  }
  
  public SubQuery getSubQuery(SubQueryId id) {
    return this.subqueries.get(id);
  }
  
  public Collection<SubQuery> getSubQueries() {
    return this.subqueries.values();
  }
  
  public QueryUnit getQueryUnit(QueryUnitId id) {
    return this.getSubQuery(id.getSubQueryId()).getQueryUnit(id);
  }

  public QueryStatus getStatus() {
    return this.status;
  }

  public void setStatus(QueryStatus status) {
    this.status = status;
  }
}
