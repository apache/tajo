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

package org.apache.tajo.client;

import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.ipc.ClientProtos.GetQueryStatusResponse;
import org.apache.tajo.util.VersionInfo;

public class QueryStatus {
  private QueryId queryId;
  private QueryState state;
  private float progress;
  private long submitTime;
  private long finishTime;
  private boolean hasResult;
  private String errorText;
  private String errorTrace;
  private String queryMasterHost;
  private int queryMasterPort;

  public QueryStatus(GetQueryStatusResponse proto) {
    queryId = new QueryId(proto.getQueryId());
    state = proto.getQueryState();
    progress = proto.getProgress();
    submitTime = proto.getSubmitTime();
    finishTime = proto.getFinishTime();
    hasResult = proto.getHasResult();
    if (proto.hasErrorMessage()) {
      errorText = proto.getErrorMessage();
    } else {
      errorText = "Internal error. Please check out log files in ${tajo_install_dir}/logs files.";
    }
    if (proto.hasErrorTrace()) {
      errorTrace = proto.getErrorTrace();
    }

    queryMasterHost = proto.getQueryMasterHost();
    queryMasterPort = proto.getQueryMasterPort();
  }

  public String getQueryMasterHost() {
    return queryMasterHost;
  }

  public int getQueryMasterPort() {
    return queryMasterPort;
  }

  public QueryId getQueryId() {
    return this.queryId;
  }

  public QueryState getState() {
    return this.state;
  }

  public float getProgress() {
    return progress;
  }

  public long getSubmitTime() {
    return this.submitTime;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public boolean hasResult() {
    return this.hasResult;
  }

  public String getErrorMessage() {
    return errorText;
  }

  public String getErrorTrace() {
    return errorTrace;
  }

}
