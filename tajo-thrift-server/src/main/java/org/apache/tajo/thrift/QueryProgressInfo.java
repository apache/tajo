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
import org.apache.tajo.thrift.generated.TGetQueryStatusResponse;

public class QueryProgressInfo {
  QueryId queryId;
  String query;
  TGetQueryStatusResponse queryStatus;
  SessionIdProto sessionId;
  long lastTouchTime;

  public QueryProgressInfo() {
  }

  public QueryProgressInfo(SessionIdProto sessionId) {
    this.sessionId = sessionId;
  }

  public QueryProgressInfo clone() {
    QueryProgressInfo cloneInfo = new QueryProgressInfo();

    cloneInfo.queryId = new QueryId(queryId.getProto());
    cloneInfo.query = query;
    cloneInfo.lastTouchTime = lastTouchTime;
    cloneInfo.sessionId = SessionIdProto.newBuilder().mergeFrom(sessionId).build();

    if (queryStatus != null) {
      cloneInfo.queryStatus = queryStatus;
    }
    return cloneInfo;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public String getQuery() {
    return query;
  }

  public TGetQueryStatusResponse getQueryStatus() {
    return queryStatus;
  }

  public SessionIdProto getSessionId() {
    return sessionId;
  }

  public long getLastTouchTime() {
    return lastTouchTime;
  }
}
