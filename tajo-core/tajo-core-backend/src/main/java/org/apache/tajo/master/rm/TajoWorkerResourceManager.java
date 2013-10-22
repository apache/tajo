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

package org.apache.tajo.master.rm;

import com.google.protobuf.RpcCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.master.querymaster.QueryJobEvent;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class TajoWorkerResourceManager implements WorkerResourceManager {
  private static final Log LOG = LogFactory.getLog(TajoWorkerResourceManager.class);

  private TajoMaster.MasterContext masterContext;

  private Map<String, WorkerResource> allWorkerResourceMap = new HashMap<String, WorkerResource>();
  private Set<String> liveWorkerResources = new HashSet<String>();
  private Set<String> deadWorkerResources = new HashSet<String>();

  private Map<QueryId, WorkerResource> queryMasterMap = new HashMap<QueryId, WorkerResource>();

  private final Object workerResourceLock = new Object();

  private final String queryIdSeed;

  private WorkerResourceAllocationThread workerResourceAllocator;

  private final BlockingQueue<WorkerResourceRequest> requestQueue;

  private final List<WorkerResourceRequest> reAllocationList;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private int queryMasterMemoryMB;

  private int queryMasterDiskSlot;

  public TajoWorkerResourceManager(TajoMaster.MasterContext masterContext) {
    this.masterContext = masterContext;
    this.queryIdSeed = String.valueOf(System.currentTimeMillis());
    this.queryMasterMemoryMB = masterContext.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_QUERY_MASTER_MEMORY_MB);
    this.queryMasterDiskSlot = masterContext.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_QUERY_MASTER_DISKS);

    requestQueue = new LinkedBlockingDeque<WorkerResourceRequest>();
    reAllocationList = new ArrayList<WorkerResourceRequest>();

    workerResourceAllocator = new WorkerResourceAllocationThread();
    workerResourceAllocator.start();
  }

  public Map<String, WorkerResource> getWorkers() {
    return Collections.unmodifiableMap(allWorkerResourceMap);
  }

  public int getNumClusterSlots() {
    int numSlots = 0;
    synchronized(workerResourceLock) {
      for(String eachWorker: liveWorkerResources) {
        numSlots += allWorkerResourceMap.get(eachWorker).getSlots();
      }
    }

    return numSlots;
  }

  @Override
  public void stop() {
    if(stopped.get()) {
      return;
    }
    stopped.set(true);
    if(workerResourceAllocator != null) {
      workerResourceAllocator.interrupt();
    }
  }

  @Override
  public WorkerResource allocateQueryMaster(QueryInProgress queryInProgress) {
    List<WorkerResource> workerResources = chooseWorkers(true, queryMasterMemoryMB, queryMasterDiskSlot, 1);
    if(workerResources.size() == 0) {
      //TODO if resource available, assign worker.
      LOG.warn("No available resource for querymaster:" + queryInProgress.getQueryId());
      return null;
    }
    WorkerResource queryMasterWorker = workerResources.get(0);
    synchronized(workerResourceLock) {
      queryMasterMap.put(queryInProgress.getQueryId(), queryMasterWorker);
    }
    LOG.info(queryInProgress.getQueryId() + "'s QueryMaster is " + queryMasterWorker);
    return queryMasterWorker;
  }

  @Override
  public void startQueryMaster(QueryInProgress queryInProgress) {
    WorkerResource queryMasterWorkerResource = null;
    synchronized(workerResourceLock) {
      queryMasterWorkerResource = queryMasterMap.get(queryInProgress.getQueryId());
    }

    if(queryMasterWorkerResource != null) {
      startQueryMaster(queryInProgress.getQueryId(), queryMasterWorkerResource);
    } else {
      //add queue
      TajoMasterProtocol.WorkerResourceAllocationRequest request =
          TajoMasterProtocol.WorkerResourceAllocationRequest.newBuilder()
            .setMemoryMBSlots(queryMasterMemoryMB)
            .setDiskSlots(1)
            .setExecutionBlockId(QueryIdFactory.newExecutionBlockId(QueryIdFactory.NULL_QUERY_ID, 0).getProto())
            .setNumWorks(1)
            .build();
      try {
        requestQueue.put(new WorkerResourceRequest(queryInProgress.getQueryId(), true, request, null));
      } catch (InterruptedException e) {
      }
    }
  }

  private void startQueryMaster(QueryId queryId, WorkerResource workResource) {
    QueryInProgress queryInProgress = masterContext.getQueryJobManager().getQueryInProgress(queryId);
    if(queryInProgress == null) {
      LOG.warn("No QueryInProgress while starting  QueryMaster:" + queryId);
      return;
    }
    queryInProgress.getQueryInfo().setQueryMasterResource(workResource);

    //fire QueryJobStart event
    queryInProgress.getEventHandler().handle(
        new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_START, queryInProgress.getQueryInfo()));
  }

  @Override
  public String getSeedQueryId() throws IOException {
    return queryIdSeed;
  }

  @Override
  public void allocateWorkerResources(
      TajoMasterProtocol.WorkerResourceAllocationRequest request,
      RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> callBack) {
    try {
      //TODO checking queue size
      requestQueue.put(new WorkerResourceRequest(
          new QueryId(request.getExecutionBlockId().getQueryId()), false, request, callBack));
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  class WorkerResourceRequest {
    boolean queryMasterRequest;
    QueryId queryId;
    TajoMasterProtocol.WorkerResourceAllocationRequest request;
    RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> callBack;
    WorkerResourceRequest(
        QueryId queryId,
        boolean queryMasterRequest, TajoMasterProtocol.WorkerResourceAllocationRequest request,
        RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> callBack) {
      this.queryId = queryId;
      this.queryMasterRequest = queryMasterRequest;
      this.request = request;
      this.callBack = callBack;
    }
  }

  class WorkerResourceAllocationThread extends Thread {
    @Override
    public void run() {
      LOG.info("WorkerResourceAllocationThread start");
      while(!stopped.get()) {
        try {
          WorkerResourceRequest resourceRequest = requestQueue.take();

          if (LOG.isDebugEnabled()) {
            LOG.debug("allocateWorkerResources:" +
                (new ExecutionBlockId(resourceRequest.request.getExecutionBlockId())) +
                ", required:" + resourceRequest.request.getNumWorks() +
                ", queryMasterRequest=" + resourceRequest.queryMasterRequest +
                ", liveWorkers=" + liveWorkerResources.size());
          }

          List<WorkerResource> workerResources = chooseWorkers(false,
              resourceRequest.request.getMemoryMBSlots(),
              resourceRequest.request.getDiskSlots(),
              resourceRequest.request.getNumWorks());

          LOG.debug("allocateWorkerResources: allocated:" + workerResources.size());

          if(workerResources.size() > 0) {
            if(resourceRequest.queryMasterRequest) {
              startQueryMaster(resourceRequest.queryId, workerResources.get(0));
            } else {
              List<TajoMasterProtocol.WorkerAllocatedResource> workerHosts =
                  new ArrayList<TajoMasterProtocol.WorkerAllocatedResource>();

              for(WorkerResource eachWorker: workerResources) {
                workerHosts.add(TajoMasterProtocol.WorkerAllocatedResource.newBuilder()
                    .setWorkerHostAndPort(eachWorker.getAllocatedHost() + ":" + eachWorker.getPeerRpcPort())
                    .setWorkerPullServerPort(eachWorker.getPullServerPort())
                    .build());
              }
              resourceRequest.callBack.run(TajoMasterProtocol.WorkerResourceAllocationResponse.newBuilder()
                  .setExecutionBlockId(resourceRequest.request.getExecutionBlockId())
                  .addAllWorkerAllocatedResource(workerHosts)
                  .build()
              );
            }
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("=========================================");
              LOG.debug("Available Workers");
              for(String liveWorker: liveWorkerResources) {
                LOG.debug(allWorkerResourceMap.get(liveWorker).toString());
              }
              LOG.debug("=========================================");
            }
            requestQueue.put(resourceRequest);
            Thread.sleep(100);
          }
        } catch(InterruptedException ie) {
          LOG.error(ie);
        }
      }
    }
  }

  private List<WorkerResource> chooseWorkers(boolean queryMaster,
                                             int requiredMemoryMBSlots, int requiredDiskSlots,
                                             int numWorkerSlots) {
    List<WorkerResource> selectedWorkers = new ArrayList<WorkerResource>();

    int selectedCount = 0;

    synchronized(workerResourceLock) {
      List<String> randomWorkers = new ArrayList<String>(liveWorkerResources);
      Collections.shuffle(randomWorkers);
      int liveWorkerSize = randomWorkers.size();
      Set<String> insufficientWorkers = new HashSet<String>();
      boolean stop = false;
      while(!stop) {
        if(insufficientWorkers.size() >= liveWorkerSize || selectedCount >= numWorkerSlots) {
          break;
        }
        for(String eachWorker: randomWorkers) {
          if(insufficientWorkers.size() >= liveWorkerSize || selectedCount >= numWorkerSlots) {
            stop = true;
          } else {
            WorkerResource workerResource = allWorkerResourceMap.get(eachWorker);
            if(workerResource.getAvailableMemoryMBSlots() >= requiredMemoryMBSlots) {
                //TODO check disk slot
                // && workerResource.getAvailableDiskSlots() >= requiredDiskSlots) {
              if(queryMaster && workerResource.isQueryMasterAllocated()) {
                insufficientWorkers.add(eachWorker);
                continue;
              }
              workerResource.addUsedMemoryMBSlots(requiredMemoryMBSlots);
              //workerResource.addUsedDiskSlots(requiredDiskSlots);
              workerResource.setQueryMasterAllocated(queryMaster);
              selectedWorkers.add(workerResource);
              selectedCount++;
            } else {
              insufficientWorkers.add(eachWorker);
            }
          }
        }
        if(!stop) {
          for(String eachWorker: insufficientWorkers) {
            randomWorkers.remove(eachWorker);
          }
        }
      }
    }

    return selectedWorkers;
  }

  @Override
  public void releaseWorkerResource(QueryId queryId, WorkerResource workerResource) {
    synchronized(workerResourceLock) {
      WorkerResource managedWorkerResource = allWorkerResourceMap.get(workerResource.getId());
      if(managedWorkerResource != null) {
        managedWorkerResource.releaseResource(workerResource);
      }
    }

    synchronized(reAllocationList) {
      reAllocationList.notifyAll();
    }
  }

  @Override
  public boolean isQueryMasterStopped(QueryId queryId) {
    synchronized(workerResourceLock) {
      return !queryMasterMap.containsKey(queryId);
    }
  }

  @Override
  public void init(Configuration conf) {
  }

  @Override
  public void stopQueryMaster(QueryId queryId) {
    WorkerResource queryMasterWorkerResource = null;
    synchronized(workerResourceLock) {
      if(!queryMasterMap.containsKey(queryId)) {
        LOG.warn("No QueryMaster resource info for " + queryId);
        return;
      } else {
        queryMasterWorkerResource = queryMasterMap.remove(queryId);
      }
    }

    WorkerResource workerResource = new WorkerResource();
    workerResource.copyId(queryMasterWorkerResource);
    workerResource.setMemoryMBSlots(queryMasterMemoryMB);
    workerResource.setDiskSlots(queryMasterDiskSlot);
    workerResource.setCpuCoreSlots(0);
    workerResource.setQueryMasterAllocated(queryMasterWorkerResource.isQueryMasterAllocated());
    releaseWorkerResource(queryId, workerResource);
    LOG.info("release QueryMaster resource:" + queryId + "," + queryMasterWorkerResource);
  }

  public void workerHeartbeat(TajoMasterProtocol.TajoHeartbeat request) {
    synchronized(workerResourceLock) {
      String hostAndPort = request.getTajoWorkerHost() + ":" + request.getTajoWorkerPort();
      if(allWorkerResourceMap.containsKey(hostAndPort)) {
        if(deadWorkerResources.contains(hostAndPort)) {
          deadWorkerResources.remove(hostAndPort);
          liveWorkerResources.add(hostAndPort);
        }
        WorkerResource workerResource = allWorkerResourceMap.get(hostAndPort);
        workerResource.setLastHeartbeat(System.currentTimeMillis());
        workerResource.setWorkerStatus(WorkerStatus.LIVE);
        workerResource.setNumRunningTasks(request.getServerStatus().getRunningTaskNum());
        workerResource.setMaxHeap(request.getServerStatus().getJvmHeap().getMaxHeap());
        workerResource.setFreeHeap(request.getServerStatus().getJvmHeap().getFreeHeap());
        workerResource.setTotalHeap(request.getServerStatus().getJvmHeap().getTotalHeap());
      } else {
        WorkerResource workerResource = new WorkerResource();
        workerResource.setAllocatedHost(request.getTajoWorkerHost());

        int[] ports = new int[] { request.getTajoWorkerPort(), request.getTajoWorkerClientPort() };

        workerResource.setPeerRpcPort(request.getTajoWorkerPort());
        workerResource.setClientPort(request.getTajoWorkerClientPort());
        workerResource.setPullServerPort(request.getTajoWorkerPullServerPort());
        workerResource.setHttpPort(request.getTajoWorkerHttpPort());

        workerResource.setLastHeartbeat(System.currentTimeMillis());
        workerResource.setWorkerStatus(WorkerStatus.LIVE);
        if(request.getServerStatus() != null) {
          workerResource.setMemoryMBSlots(request.getServerStatus().getSystem().getTotalMemoryMB());
          workerResource.setCpuCoreSlots(request.getServerStatus().getSystem().getAvailableProcessors());
          workerResource.setDiskSlots(request.getServerStatus().getDiskSlots());
          workerResource.setNumRunningTasks(request.getServerStatus().getRunningTaskNum());
          workerResource.setMaxHeap(request.getServerStatus().getJvmHeap().getMaxHeap());
          workerResource.setFreeHeap(request.getServerStatus().getJvmHeap().getFreeHeap());
          workerResource.setTotalHeap(request.getServerStatus().getJvmHeap().getTotalHeap());
        } else {
          workerResource.setMemoryMBSlots(4096);
          workerResource.setDiskSlots(4);
          workerResource.setCpuCoreSlots(4);
        }

        allWorkerResourceMap.put(workerResource.getId(), workerResource);
        liveWorkerResources.add(hostAndPort);

        LOG.info("TajoWorker:" + workerResource + " added in live TajoWorker list");

        workerResourceLock.notifyAll();
      }
    }
  }
}
