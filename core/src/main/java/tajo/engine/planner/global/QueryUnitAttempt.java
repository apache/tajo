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

package tajo.engine.planner.global;

import tajo.QueryUnitAttemptId;
import tajo.catalog.statistics.TableStat;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.master.AbstractQuery;

public class QueryUnitAttempt extends AbstractQuery {
  private final static int EXPIRE_TIME = 15000;

  private final QueryUnitAttemptId id;
  private final QueryUnit queryUnit;

  private String hostName;
  private int expire;
  private QueryStatus status;

  public QueryUnitAttempt(QueryUnitAttemptId id, QueryUnit queryUnit) {
    this.id = id;
    this.expire = QueryUnitAttempt.EXPIRE_TIME;
    this.queryUnit = queryUnit;
  }

  public QueryUnitAttemptId getId() {
    return this.id;
  }

  public QueryUnit getQueryUnit() {
    return this.queryUnit;
  }

  public QueryStatus getStatus() {
    return status;
  }

  public String getHost() {
    return this.hostName;
  }

  public void setStatus(QueryStatus status) {
    this.status = status;
  }

  public void setHost(String host) {
    this.hostName = host;
  }

  /*
    * Expire time
    */

  public synchronized void setExpireTime(int expire) {
    this.expire = expire;
  }

  public synchronized void updateExpireTime(int period) {
    this.setExpireTime(this.expire - period);
  }

  public synchronized void resetExpireTime() {
    this.setExpireTime(QueryUnitAttempt.EXPIRE_TIME);
  }

  public int getLeftTime() {
    return this.expire;
  }

  public void updateProgress(TaskStatusProto progress) {
    if (status != progress.getStatus()) {
      this.setProgress(progress.getProgress());
      this.setStatus(progress.getStatus());
      if (progress.getPartitionsCount() > 0) {
        this.getQueryUnit().setPartitions(progress.getPartitionsList());
      }
      if (progress.hasResultStats()) {
        this.getQueryUnit().setStats(new TableStat(progress.getResultStats()));
      }
    }
    this.resetExpireTime();
  }
}
