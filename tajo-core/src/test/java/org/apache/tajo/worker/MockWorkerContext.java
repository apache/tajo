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

package org.apache.tajo.worker;

import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.querymaster.QueryMaster;
import org.apache.tajo.querymaster.QueryMasterManagerService;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.util.history.HistoryReader;
import org.apache.tajo.util.history.HistoryWriter;
import org.apache.tajo.util.metrics.TajoSystemMetrics;

public abstract class MockWorkerContext implements TajoWorker.WorkerContext {
  TajoSystemMetrics tajoSystemMetrics;

  @Override
  public QueryMaster getQueryMaster() {
    return null;
  }

  public abstract TajoConf getConf();

  @Override
  public ServiceTracker getServiceTracker() {
    return null;
  }

  @Override
  public QueryMasterManagerService getQueryMasterManagerService() {
    return null;
  }

  @Override
  public TaskRunnerManager getTaskRunnerManager() {
    return null;
  }

  @Override
  public CatalogService getCatalog() {
    return null;
  }

  @Override
  public WorkerConnectionInfo getConnectionInfo() {
    return null;
  }

  @Override
  public String getWorkerName() {
    return null;
  }

  @Override
  public LocalDirAllocator getLocalDirAllocator() {
    return null;
  }

  @Override
  public QueryCoordinatorProtocol.ClusterResourceSummary getClusterResource() {
    return null;
  }

  @Override
  public TajoSystemMetrics getWorkerSystemMetrics() {

    if (tajoSystemMetrics == null) {
      tajoSystemMetrics = new TajoSystemMetrics(getConf(), "test-file-group", "localhost");
      tajoSystemMetrics.start();
    }
    return tajoSystemMetrics;
  }

  @Override
  public HashShuffleAppenderManager getHashShuffleAppenderManager() {
    return null;
  }

  @Override
  public HistoryWriter getTaskHistoryWriter() {
    return null;
  }

  @Override
  public HistoryReader getHistoryReader() {
    return null;
  }

  @Override
  public void cleanup(String strPath) {

  }

  @Override
  public void cleanupTemporalDirectories() {

  }

  @Override
  public void setClusterResource(QueryCoordinatorProtocol.ClusterResourceSummary clusterResource) {

  }

  @Override
  public void setNumClusterNodes(int numClusterNodes) {

  }
}

