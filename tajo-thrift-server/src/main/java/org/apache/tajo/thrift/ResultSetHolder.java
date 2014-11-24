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

package org.apache.tajo.thrift;

import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos.SessionIdProto;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.jdbc.TajoResultSetBase;

public class ResultSetHolder {
  private SessionIdProto sessionId;
  private QueryId queryId;
  private TajoResultSetBase resultSet;
  private TableDesc tableDesc;
  private Schema schema;
  private long lastTouchTime;

  public String getKey() {
    return getKey(sessionId, queryId);
  }

  public static String getKey(SessionIdProto sessionId, QueryId queryId) {
    return sessionId.getId() + "," + queryId.toString();
  }

  public SessionIdProto getSessionId() {
    return sessionId;
  }

  public void setSessionId(SessionIdProto sessionId) {
    this.sessionId = sessionId;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public void setQueryId(QueryId queryId) {
    this.queryId = queryId;
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public void setTableDesc(TableDesc tableDesc) {
    this.tableDesc = tableDesc;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public long getLastTouchTime() {
    return lastTouchTime;
  }

  public void setLastTouchTime(long lastTouchTime) {
    this.lastTouchTime = lastTouchTime;
  }

  public TajoResultSetBase getResultSet() {
    return resultSet;
  }

  public void setResultSet(TajoResultSetBase resultSet) {
    this.resultSet = resultSet;
  }
}
