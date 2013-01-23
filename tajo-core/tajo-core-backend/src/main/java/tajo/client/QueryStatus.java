/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.client;

import tajo.QueryId;
import tajo.TajoProtos.QueryState;
import tajo.client.ClientProtocol.GetQueryStatusResponse;

public class QueryStatus {
  private QueryId queryId;
  private QueryState state;
  private float progress;
  private long submitTime;
  private long initTime;
  private long finishTime;
  private boolean hasResult;
  private String errorText;

  public QueryStatus(GetQueryStatusResponse proto) {
    queryId = new QueryId(proto.getQueryId());
    state = proto.getState();
    progress = proto.getProgress();
    submitTime = proto.getSubmitTime();
    initTime = proto.getInitTime();
    finishTime = proto.getFinishTime();
    hasResult = proto.getHasResult();
    if (proto.hasErrorMessage()) {
      errorText = proto.getErrorMessage();
    }
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

  public long getInitTime() {
    return this.initTime;
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
}
