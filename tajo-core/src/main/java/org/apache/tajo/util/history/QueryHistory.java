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

package org.apache.tajo.util.history;

import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.ipc.ClientProtos.QueryHistoryProto;
import org.apache.tajo.ipc.ClientProtos.StageHistoryProto;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueProto;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryHistory implements GsonObject, History {
  @Expose
  private String queryId;
  @Expose
  private String queryMaster;
  @Expose
  private int httpPort;
  @Expose
  private List<String[]> sessionVariables;
  @Expose
  private String logicalPlan;
  @Expose
  private String distributedPlan;
  @Expose
  private List<StageHistory> stageHistories;

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public void setQueryMaster(String queryMaster) {
    this.queryMaster = queryMaster;
  }

  public void setStageHistories(List<StageHistory> stageHistories) {
    this.stageHistories = stageHistories;
  }

  public String getQueryMaster() {
    return queryMaster;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  public List<StageHistory> getStageHistories() {
    return stageHistories;
  }

  public List<String[]> getSessionVariables() {
    return sessionVariables;
  }

  public String getLogicalPlan() {
    return logicalPlan;
  }

  public String getDistributedPlan() {
    return distributedPlan;
  }

  public void setSessionVariables(List<String[]> sessionVariables) {
    this.sessionVariables = sessionVariables;
  }

  public void setLogicalPlan(String logicalPlan) {
    this.logicalPlan = logicalPlan;
  }

  public void setDistributedPlan(String distributedPlan) {
    this.distributedPlan = distributedPlan;
  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, QueryHistory.class);
  }

  @Override
  public HistoryType getHistoryType() {
    return HistoryType.QUERY;
  }

  public static QueryHistory fromJson(String json) {
    return CoreGsonHelper.fromJson(json, QueryHistory.class);
  }

  public QueryHistoryProto getProto() {
    QueryHistoryProto.Builder builder = QueryHistoryProto.newBuilder();

    builder.setQueryId(queryId)
      .setQueryMaster(queryMaster)
      .setHttpPort(httpPort)
      .setLogicalPlan(logicalPlan)
      .setDistributedPlan(distributedPlan);

    List<KeyValueProto> sessionProtos = new ArrayList<>();

    if (sessionVariables != null) {
      KeyValueProto.Builder keyValueBuilder = KeyValueProto.newBuilder();
      for (String[] eachSessionVal: sessionVariables) {
        keyValueBuilder.clear();
        keyValueBuilder.setKey(eachSessionVal[0]);
        keyValueBuilder.setValue(eachSessionVal[1]);

        sessionProtos.add(keyValueBuilder.build());
      }
    }
    builder.addAllSessionVariables(sessionProtos);


    List<StageHistoryProto> stageHistoryProtos = new ArrayList<>();
    if (stageHistories != null) {
      stageHistoryProtos.addAll(stageHistories.stream().map(eachStage -> (eachStage.getProto())).collect(Collectors.toList()));
    }
    builder.addAllStageHistories(stageHistoryProtos);

    return builder.build();
  }
}
