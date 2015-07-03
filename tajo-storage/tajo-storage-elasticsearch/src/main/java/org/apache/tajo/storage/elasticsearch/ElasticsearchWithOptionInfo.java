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

package org.apache.tajo.storage.elasticsearch;

import org.apache.tajo.catalog.TableMeta;

public class ElasticsearchWithOptionInfo {
  public String clusterName;
  public String nodes;
  public String index;
  public String type;
  public String fetchSize;
  public String primaryShard;
  public String replicaShard;
  public String pingTimeout;
  public String connectTimeout;
  public String threadPoolRecovery;
  public String threadPoolBulk;
  public String threadPoolReg;
  public String timeScroll;
  public String timeAction;

  public ElasticsearchWithOptionInfo(TableMeta tableMeta) {
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_CLUSTER) ) this.clusterName = tableMeta.getOption(ElasticsearchConstants.OPT_CLUSTER);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_NODES) ) this.nodes = tableMeta.getOption(ElasticsearchConstants.OPT_NODES);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_INDEX) ) this.index = tableMeta.getOption(ElasticsearchConstants.OPT_INDEX);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_TYPE) ) this.type = tableMeta.getOption(ElasticsearchConstants.OPT_TYPE);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_FETCH_SIZE) ) this.fetchSize = tableMeta.getOption(ElasticsearchConstants.OPT_FETCH_SIZE);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_PRIMARY_SHARD) ) this.primaryShard = tableMeta.getOption(ElasticsearchConstants.OPT_PRIMARY_SHARD);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_REPLICA_SHARD) ) this.replicaShard = tableMeta.getOption(ElasticsearchConstants.OPT_REPLICA_SHARD);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_PING_TIMEOUT) ) this.pingTimeout = tableMeta.getOption(ElasticsearchConstants.OPT_PING_TIMEOUT);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_CONNECT_TIMEOUT) ) this.connectTimeout = tableMeta.getOption(ElasticsearchConstants.OPT_CONNECT_TIMEOUT);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_THREADPOOL_RECOVERY) ) this.threadPoolRecovery = tableMeta.getOption(ElasticsearchConstants.OPT_THREADPOOL_RECOVERY);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_THREADPOOL_BULK) ) this.threadPoolBulk = tableMeta.getOption(ElasticsearchConstants.OPT_THREADPOOL_BULK);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_THREADPOOL_REG) ) this.threadPoolReg = tableMeta.getOption(ElasticsearchConstants.OPT_THREADPOOL_REG);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_TIME_SCROLL) ) this.timeScroll = tableMeta.getOption(ElasticsearchConstants.OPT_TIME_SCROLL);
    if ( tableMeta.containsOption(ElasticsearchConstants.OPT_TIME_ACTION) ) this.timeAction = tableMeta.getOption(ElasticsearchConstants.OPT_TIME_ACTION);
  }

  public String getClusterName() {
    if ( clusterName == null || clusterName.isEmpty() ) {
      clusterName = ElasticsearchConstants.CLUSTER_NAME;
    }

    return clusterName;
  }

  public String cluster() {
    return getClusterName();
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getNodes() {
    if ( nodes == null || nodes.isEmpty() ) {
      nodes = ElasticsearchConstants.NODES;
    }

    return nodes;
  }

  public String nodes() {
    return getNodes();
  }

  public void setNodes(String nodes) {
    this.nodes = nodes;
  }

  public String getIndex() {
    if ( index == null || index.isEmpty() ) {
      index = ElasticsearchConstants.INDEX_TYPE;
    }

    return index;
  }

  public String index() {
    return getIndex();
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public String getType() {
    if ( type == null || type.isEmpty() ) {
      type = ElasticsearchConstants.INDEX_TYPE;
    }

    return type;
  }

  public String type() {
    return getType();
  }

  public void setType(String type) {
    this.type = type;
  }

  public int getFetchSize() {
    if ( fetchSize == null || fetchSize.isEmpty() ) {
      fetchSize = ElasticsearchConstants.FETCH_SIZE;
    }

    return Integer.parseInt(fetchSize);
  }

  public int fetchSize() {
    return getFetchSize();
  }

  public void setFetchSize(String fetchSize) {
    this.fetchSize = fetchSize;
  }

  public String getPrimaryShard() {
    if ( primaryShard == null || primaryShard.isEmpty() ) {
      primaryShard = ElasticsearchConstants.PRIMARY_SHARD;
    }

    return primaryShard;
  }

  public String primaryShard() {
    return getPrimaryShard();
  }

  public void setPrimaryShard(String primaryShard) {
    this.primaryShard = primaryShard;
  }

  public String getReplicaShard() {
    if ( replicaShard == null || replicaShard.isEmpty() ) {
      replicaShard = ElasticsearchConstants.REPLICA_SHARD;
    }

    return replicaShard;
  }

  public String replicaShard() {
    return getReplicaShard();
  }

  public void setReplicaShard(String replicaShard) {
    this.replicaShard = replicaShard;
  }

  public String getPingTimeout() {
    if ( pingTimeout == null || pingTimeout.isEmpty() ) {
      pingTimeout = ElasticsearchConstants.PING_TIMEOUT;
    }

    return pingTimeout;
  }

  public String pingTimeout() {
    return getPingTimeout();
  }

  public void setPingTimeout(String pingTimeout) {
    this.pingTimeout = pingTimeout;
  }

  public String getConnectTimeout() {
    if ( connectTimeout == null || connectTimeout.isEmpty() ) {
      connectTimeout = ElasticsearchConstants.CONNECT_TIMEOUT;
    }

    return connectTimeout;
  }

  public String connectTimeout() {
    return getConnectTimeout();
  }

  public void setConnectTimeout(String connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public String getThreadPoolRecovery() {
    if ( threadPoolRecovery == null || threadPoolRecovery.isEmpty() ) {
      threadPoolRecovery = ElasticsearchConstants.THREADPOOL_RECOVERY;
    }

    return threadPoolRecovery;
  }

  public String threadPoolRecovery() {
    return getThreadPoolRecovery();
  }

  public void setThreadPoolRecovery(String threadPoolRecovery) {
    this.threadPoolRecovery = threadPoolRecovery;
  }

  public String getThreadPoolBulk() {
    if ( threadPoolBulk == null || threadPoolBulk.isEmpty() ) {
      threadPoolBulk = ElasticsearchConstants.THREADPOOL_BULK;
    }

    return threadPoolBulk;
  }

  public String threadPoolBulk() {
    return getThreadPoolBulk();
  }

  public void setThreadPoolBulk(String threadPoolBulk) {
    this.threadPoolBulk = threadPoolBulk;
  }

  public String getThreadPoolReg() {
    if ( threadPoolReg == null || threadPoolReg.isEmpty() ) {
      threadPoolReg = ElasticsearchConstants.THREADPOOL_REG;
    }

    return threadPoolReg;
  }

  public String threadPoolReg() {
    return getThreadPoolReg();
  }

  public void setThreadPoolReg(String threadPoolReg) {
    this.threadPoolReg = threadPoolReg;
  }

  public String getTimeScroll() {
    if ( timeScroll == null || timeScroll.isEmpty() ) {
      timeScroll = ElasticsearchConstants.TIME_SCROLL;
    }

    return timeScroll;
  }

  public String timeScroll() {
    return getTimeScroll();
  }

  public void setTimeScroll(String timeScroll) {
    this.timeScroll = timeScroll;
  }

  public String getTimeAction() {
    if ( timeAction == null || timeAction.isEmpty() ) {
      timeAction = ElasticsearchConstants.TIME_ACTION;
    }

    return timeAction;
  }

  public String timeAction() {
    return getTimeAction();
  }

  public void setTimeAction(String timeAction) {
    this.timeAction = timeAction;
  }
}
