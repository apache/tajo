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

package org.apache.tajo.master;


import com.google.gson.annotations.Expose;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.ClientProtos.QueryInfoProto;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.util.history.History;

public class QueryInfo implements GsonObject, History, Comparable<QueryInfo> {
  private QueryId queryId;
  @Expose
  private QueryContext context;
  @Expose
  private String sql;
  @Expose
  private volatile TajoProtos.QueryState queryState;
  @Expose
  private volatile float progress;
  @Expose
  private volatile long startTime;
  @Expose
  private volatile  long finishTime;

  @Deprecated
  @Expose
  private String lastMessage;
  @Expose
  private String hostNameOfQM;
  @Expose
  private int queryMasterPort;
  @Expose
  private int queryMasterClientPort;
  @Expose
  private int queryMasterInfoPort;
  @Expose
  private String queryIdStr;
  @Expose
  private volatile TableDesc resultDesc;

  private String jsonExpr;

  public QueryInfo(QueryId queryId) {
    this(queryId, null, null, null);
  }

  public QueryInfo(QueryId queryId, QueryContext queryContext, String sql, String jsonExpr) {
    this.queryId = queryId;
    this.queryIdStr = queryId.toString();
    this.context = queryContext;
    this.sql = sql;
    this.jsonExpr = jsonExpr;

    this.queryState = TajoProtos.QueryState.QUERY_MASTER_INIT;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public QueryContext getQueryContext() {
    return context;
  }

  public String getSql() {
    return sql;
  }

  public String getQueryMasterHost() {
    return hostNameOfQM;
  }

  public void setQueryMaster(String hostName) {
    this.hostNameOfQM = hostName;
  }

  public int getQueryMasterInfoPort() {
    return queryMasterInfoPort;
  }

  public void setQueryMasterInfoPort(int queryMasterInfoPort) {
    this.queryMasterInfoPort = queryMasterInfoPort;
  }

  public void setQueryMasterPort(int port) {
    this.queryMasterPort = port;
  }

  public int getQueryMasterPort() {
    return queryMasterPort;
  }

  public void setQueryMasterclientPort(int port) {
    queryMasterClientPort = port;
  }

  public int getQueryMasterClientPort() {
    return queryMasterClientPort;
  }

  public TajoProtos.QueryState getQueryState() {
    return queryState;
  }

  public void setQueryState(TajoProtos.QueryState queryState) {
    this.queryState = queryState;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public String getLastMessage() {
    return lastMessage;
  }

  public void setLastMessage(String lastMessage) {
    this.lastMessage = lastMessage;
  }

  public float getProgress() {
    return progress;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  public void setResultDesc(TableDesc result) {
    this.resultDesc = result;
  }

  public boolean hasResultdesc() {
    return resultDesc != null;
  }

  public TableDesc getResultDesc() {
    return resultDesc;
  }

  @Override
  public String toString() {
    return queryId.toString() + ",state=" + queryState +",progress=" + progress + ", queryMaster="
        + getQueryMasterHost();
  }

  public String getJsonExpr() {
    return jsonExpr;
  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, QueryInfo.class);
  }

  @Override
  public HistoryType getHistoryType() {
    return HistoryType.QUERY_SUMMARY;
  }

  public static QueryInfo fromJson(String json) {
    QueryInfo queryInfo = CoreGsonHelper.fromJson(json, QueryInfo.class);
    queryInfo.queryId = TajoIdUtils.parseQueryId(queryInfo.queryIdStr);
    return queryInfo;
  }

  public String getQueryIdStr() {
    return queryIdStr;
  }

  public QueryInfoProto getProto() {
    QueryInfoProto.Builder builder = QueryInfoProto.newBuilder();

    builder.setQueryId(queryId.toString())
        .setQueryState(queryState)
        .setContextVars(context.getProto())
        .setProgress(progress)
        .setStartTime(startTime)
        .setFinishTime(finishTime)
        .setQueryMasterPort(queryMasterPort)
        .setQueryMasterClientPort(queryMasterClientPort)
        .setQueryMasterInfoPort(queryMasterInfoPort);

    if (resultDesc != null) {
      builder.setResultDesc(resultDesc.getProto());
    }

    if (sql != null) {
      builder.setSql(sql);
    }

    if (lastMessage != null) {
      builder.setLastMessage(lastMessage);
    }

    if (hostNameOfQM != null) {
      builder.setHostNameOfQm(hostNameOfQM);
    }

    return builder.build();
  }

  @Override
  public int compareTo(QueryInfo o) {
    return queryId.compareTo(o.queryId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    QueryInfo queryInfo = (QueryInfo) o;

    if (finishTime != queryInfo.finishTime) return false;
    if (Float.compare(queryInfo.progress, progress) != 0) return false;
    if (queryMasterClientPort != queryInfo.queryMasterClientPort) return false;
    if (queryMasterInfoPort != queryInfo.queryMasterInfoPort) return false;
    if (queryMasterPort != queryInfo.queryMasterPort) return false;
    if (startTime != queryInfo.startTime) return false;
    if (context != null ? !context.equals(queryInfo.context) : queryInfo.context != null) return false;
    if (hostNameOfQM != null ? !hostNameOfQM.equals(queryInfo.hostNameOfQM) : queryInfo.hostNameOfQM != null)
      return false;
    if (jsonExpr != null ? !jsonExpr.equals(queryInfo.jsonExpr) : queryInfo.jsonExpr != null) return false;
    if (lastMessage != null ? !lastMessage.equals(queryInfo.lastMessage) : queryInfo.lastMessage != null) return false;
    if (queryId != null ? !queryId.equals(queryInfo.queryId) : queryInfo.queryId != null) return false;
    if (queryIdStr != null ? !queryIdStr.equals(queryInfo.queryIdStr) : queryInfo.queryIdStr != null) return false;
    if (queryState != queryInfo.queryState) return false;
    if (resultDesc != null ? !resultDesc.equals(queryInfo.resultDesc) : queryInfo.resultDesc != null) return false;
    if (sql != null ? !sql.equals(queryInfo.sql) : queryInfo.sql != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = queryId != null ? queryId.hashCode() : 0;
    result = 31 * result + (context != null ? context.hashCode() : 0);
    result = 31 * result + (sql != null ? sql.hashCode() : 0);
    result = 31 * result + (queryState != null ? queryState.hashCode() : 0);
    result = 31 * result + (progress != +0.0f ? Float.floatToIntBits(progress) : 0);
    result = 31 * result + (int) (startTime ^ (startTime >>> 32));
    result = 31 * result + (int) (finishTime ^ (finishTime >>> 32));
    result = 31 * result + (lastMessage != null ? lastMessage.hashCode() : 0);
    result = 31 * result + (hostNameOfQM != null ? hostNameOfQM.hashCode() : 0);
    result = 31 * result + queryMasterPort;
    result = 31 * result + queryMasterClientPort;
    result = 31 * result + queryMasterInfoPort;
    result = 31 * result + (queryIdStr != null ? queryIdStr.hashCode() : 0);
    result = 31 * result + (resultDesc != null ? resultDesc.hashCode() : 0);
    result = 31 * result + (jsonExpr != null ? jsonExpr.hashCode() : 0);
    return result;
  }
}
