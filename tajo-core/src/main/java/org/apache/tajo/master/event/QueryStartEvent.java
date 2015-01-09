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

package org.apache.tajo.master.event;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.tajo.QueryId;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.session.Session;

/**
 * This event is conveyed to QueryMaster.
 */
public class QueryStartEvent extends AbstractEvent {
  public enum EventType {
    QUERY_START
  }

  private final QueryId queryId;
  private final Session session;
  private final QueryContext queryContext;
  private final String jsonExpr;
  private final String logicalPlanJson;

  public QueryStartEvent(QueryId queryId, Session session, QueryContext queryContext, String jsonExpr,
                         String logicalPlanJson) {
    super(EventType.QUERY_START);
    this.queryId = queryId;
    this.session = session;
    this.queryContext = queryContext;
    this.jsonExpr = jsonExpr;
    this.logicalPlanJson = logicalPlanJson;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public Session getSession() {
    return this.session;
  }

  public QueryContext getQueryContext() {
    return this.queryContext;
  }

  public String getJsonExpr() {
    return this.jsonExpr;
  }

  public String getLogicalPlanJson() {
    return logicalPlanJson;
  }

  @Override
  public String toString() {
    return getClass().getName() + "," + getType() + "," + queryId;
  }
}
