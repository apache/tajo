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

package org.apache.tajo.querymaster;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.tajo.master.QueryInfo;

public class QueryJobEvent extends AbstractEvent<QueryJobEvent.Type> {
  private QueryInfo queryInfo;

  public QueryJobEvent(Type type, QueryInfo queryInfo) {
    super(type);

    this.queryInfo = queryInfo;
  }

  public QueryInfo getQueryInfo() {
    return this.queryInfo;
  }

  public enum Type {
    QUERY_MASTER_START,
    QUERY_JOB_HEARTBEAT,
    QUERY_JOB_STOP,
    QUERY_JOB_KILL
  }
}
