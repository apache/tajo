/**
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

package org.apache.tajo.master.scheduler;

import com.google.common.base.Objects;
import org.apache.tajo.QueryId;
import org.apache.tajo.util.NumberUtil;

/**
 * A QuerySchedulingInfo represents an scheduling information.
 * It provides a common interface for queue and priority
 */

public class QuerySchedulingInfo implements Comparable<QuerySchedulingInfo> {
  /** Name of queue */
  private String queue;
  /** Query owner */
  private String user;
  private QueryId queryId;
  /** Query priority for queries in same queue */
  private int priority;
  /** Start time for query in same queue */
  private long startTime;

  public QuerySchedulingInfo(String queue, String user, QueryId queryId, int priority, long startTime) {
    this.queue = queue;
    this.user = user;
    this.queryId = queryId;
    this.priority = priority;
    this.startTime = startTime;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public String getUser() {
    return user;
  }

  public int getPriority() {
    return priority;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getName() {
    return queryId.getId();
  }

  public String getQueue() {
    return queue;
  }


  @Override
  public int compareTo(QuerySchedulingInfo o) {
    int ret = NumberUtil.compare(priority, o.priority);
    if(ret == 0) {
      ret = NumberUtil.compare(startTime, o.startTime);
    }
    return ret;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QuerySchedulingInfo other = (QuerySchedulingInfo) obj;
    if (!this.getQueryId().equals(other.getQueryId()))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(queryId, queue, user, priority, startTime);
  }
}
