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

import tajo.QueryUnitId;
import tajo.ScheduleUnitId;
import tajo.SubQueryId;
import tajo.catalog.statistics.TableStat;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.global.ScheduleUnit;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SubQuery extends AbstractQuery {

  private final SubQueryId id;
  private Map<ScheduleUnitId, ScheduleUnit> units;
  private TableStat resultStats;
  private QueryStatus status;
  
  public SubQuery(SubQueryId id) {
    this.id = id;
    units = new HashMap<ScheduleUnitId, ScheduleUnit>();
  }
  
  public void addScheduleUnit(ScheduleUnit unit) {
    units.put(unit.getId(), unit);
  }
  
  public SubQueryId getId() {
    return this.id;
  }
  
  public Iterator<ScheduleUnit> getScheduleUnitIterator() {
    return this.units.values().iterator();
  }
  
  public ScheduleUnit getScheduleUnit(ScheduleUnitId id) {
    return this.units.get(id);
  }
  
  public Collection<ScheduleUnit> getScheduleUnits() {
    return this.units.values();
  }
  
  public QueryUnit getQueryUnit(QueryUnitId id) {
    return this.getScheduleUnit(id.getScheduleUnitId()).getQueryUnit(id);
  }

  public void setTableStat(TableStat stat) {
    this.resultStats = stat;
  }

  public TableStat getTableStat() {
    return this.resultStats;
  }

  public void setStatus(QueryStatus status) {
    this.status = status;
  }

  public QueryStatus getStatus() {
    return this.status;
  }
}