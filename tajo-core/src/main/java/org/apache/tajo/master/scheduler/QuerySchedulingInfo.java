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

public class QuerySchedulingInfo {
  private QueryId queryId;
  private Integer priority;
  private Long startTime;

  public QuerySchedulingInfo(QueryId queryId, Integer priority, Long startTime) {
    this.queryId = queryId;
    this.priority = priority;
    this.startTime = startTime;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public Integer getPriority() {
    return priority;
  }

  public Long getStartTime() {
    return startTime;
  }

  public String getName() {
    return queryId.getId();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(startTime, getName(), priority);
  }
}
