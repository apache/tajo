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

  private Object workerResourceLock = new Object();

  private final String queryIdSeed;

  private WorkerResourceAllocationThread workerResourceAllocator;

  private final BlockingQueue<WorkerResourceRequest> requestQueue;

  private ReAllocationThread reAllocator;

  private final List<WorkerResourceRequest> reAllocationList;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private int queryMasterMemoryMB;

  public TajoWorkerResourceManager(TajoMaster.MasterContext masterContext) {
    this.masterContext = masterContext;
    this.queryIdSeed = String.valueOf(System.currentTimeMillis());
    this.queryMasterMemoryMB = masterContext.getConf().getInt("tajo.querymaster.memoryMB", 512);

    requestQueue = new LinkedBlockingDeque<WorkerResourceRequest>();
    reAllocationList = new ArrayList<WorkerResourceRequest>();

    workerResourceAllocator = new WorkerResourceAllocationThread();
    workerResourceAllocator.start();

    reAllocator = new ReAllocationThread();
    reAllocator.start();
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

    if(reAllocator != null) {
      reAllocator.interrupt();
    }
  }

  @Override
  public WorkerResource allocateQueryMaster(QueryInProgress queryInProgress) {
    List<WorkerResource> workerResources = chooseWorkers(true, 1, 1, 1);
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
      LOG.info("====>WorkerResourceAllocationThread start");
      while(!stopped.get()) {
        try {
          WorkerResourceRequest resourceRequest = requestQueue.take();
          List<WorkerResource> workerResources = chooseWorkers(false,
              resourceRequest.request.getMemoryMBSlots(),
              resourceRequest.request.getDiskSlots(),
              resourceRequest.request.getNumWorks());

          LOG.info("====> allocateWorkerResources:" +
              (new ExecutionBlockId(resourceRequest.request.getExecutionBlockId())) +
              ", required:" + resourceRequest.request.getNumWorks() + ", allocated:" + workerResources.size() +
              ", queryMasterRequest=" + resourceRequest.queryMasterRequest +
              ", liveWorkers=" + liveWorkerResources.size());
//          if(LOG.isDebugEnabled()) {
//            LOG.debug("====> allocateWorkerResources:" +
//                (new ExecutionBlockId(resourceRequest.request.getExecutionBlockId())) +
//                ", required:" + resourceRequest.request.getNumWorks() + ", allocated:" + workerResources.size());
//          } else {
//            LOG.info("====> allocateWorkerResources: required:" + resourceRequest.request.getNumWorks() +
//                ", allocated:" + workerResources.size() + ", queryMasterRequest=" + resourceRequest.queryMasterRequest);
//          }

          if(workerResources.size() > 0) {
            if(resourceRequest.queryMasterRequest) {
              startQueryMaster(resourceRequest.queryId, workerResources.get(0));
            } else {
              List<String> workerHosts = new ArrayList<String>();

              for(WorkerResource eachWorker: workerResources) {
                workerHosts.add(eachWorker.getAllocatedHost() + ":" + eachWorker.getPorts()[0]);
              }
              resourceRequest.callBack.run(TajoMasterProtocol.WorkerResourceAllocationResponse.newBuilder()
                  .setExecutionBlockId(resourceRequest.request.getExecutionBlockId())
                  .addAllAllocatedWorks(workerHosts)
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
          }
          if(workerResources.size() < resourceRequest.request.getNumWorks()) {
            reAllocationList.add(new WorkerResourceRequest(
                resourceRequest.queryId,
                resourceRequest.queryMasterRequest,
                TajoMasterProtocol.WorkerResourceAllocationRequest.newBuilder()
                  .setMemoryMBSlots(resourceRequest.request.getMemoryMBSlots())
                  .setDiskSlots(resourceRequest.request.getDiskSlots())
                  .setExecutionBlockId(resourceRequest.request.getExecutionBlockId())
                  .setNumWorks(resourceRequest.request.getNumWorks() - workerResources.size())
                  .build(),
                resourceRequest.callBack));
          }
        } catch(InterruptedException ie) {
        }
      }
    }
  }

  class ReAllocationThread extends Thread {
    public void run() {
      List<WorkerResourceRequest> copiedList = new ArrayList<WorkerResourceRequest>();
      while(!stopped.get()) {
        copiedList.clear();
        synchronized(reAllocationList) {
          try {
            reAllocationList.wait(3 * 1000);
          } catch (InterruptedException e) {
            if(stopped.get()) {
              break;
            }
          }
          copiedList.addAll(reAllocationList);
        }

        for(WorkerResourceRequest eachRequest: copiedList) {
          try {
            requestQueue.put(eachRequest);
          } catch (InterruptedException e) {
            break;
          }
        }
        synchronized(reAllocationList) {
          reAllocationList.clear();
        }
      }
    }
  }

  private List<WorkerResource> chooseWorkers(boolean queryMaster,
                                             int requiredMemoryMBSlots, int requiredDiskSlots,
                                             int numWorkers) {
    List<WorkerResource> selectedWorkers = new ArrayList<WorkerResource>();

    int selectedCount = 0;

    synchronized(workerResourceLock) {
      for(String eachWorker: liveWorkerResources) {
        if(selectedCount >= numWorkers) {
          break;
        }
        WorkerResource workerResource = allWorkerResourceMap.get(eachWorker);
        if(workerResource.getAvailableMemoryMBSlots() >= requiredMemoryMBSlots &&
            workerResource.getAvailableDiskSlots() >= requiredDiskSlots) {
          if(queryMaster && workerResource.isQueryMasterAllocated()) {
            continue;
          }
          workerResource.addUsedMemoryMBSlots(requiredMemoryMBSlots);
          workerResource.addUsedDiskSlots(requiredDiskSlots);
          workerResource.setQueryMasterAllocated(queryMaster);
          selectedWorkers.add(workerResource);
          selectedCount++;
        }
      }
    }

    return selectedWorkers;
  }

  public Collection<WorkerResource> getClusterWorkResources() {
    return Collections.unmodifiableCollection(allWorkerResourceMap.values());
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
    WorkerResource workerResource = null;
    synchronized(workerResourceLock) {
      workerResource = queryMasterMap.remove(queryId);
    }
    LOG.info("release QueryMaster resource:" + queryId + "," + workerResource.isQueryMasterAllocated());
    if(workerResource != null) {
      releaseWorkerResource(queryId, workerResource);
    }
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
      } else {
        WorkerResource workerResource = new WorkerResource();
        workerResource.setAllocatedHost(request.getTajoWorkerHost());

        int[] ports = new int[] { request.getTajoWorkerPort(), request.getTajoWorkerClientPort() };

        workerResource.setPorts(ports);

        workerResource.setLastHeartbeat(System.currentTimeMillis());
        workerResource.setWorkerStatus(WorkerStatus.LIVE);
        if(request.getServerStatus() != null) {
          workerResource.setMemoryMBSlots(request.getServerStatus().getSystem().getTotalMemoryMB());
          workerResource.setCpuCoreSlots(request.getServerStatus().getSystem().getAvailableProcessors());
          workerResource.setDiskSlots(request.getServerStatus().getDiskSlots());
        } else {
          workerResource.setMemoryMBSlots(4096);
          workerResource.setDiskSlots(4);
          workerResource.setCpuCoreSlots(4);
        }

        allWorkerResourceMap.put(workerResource.getId(), workerResource);
        liveWorkerResources.add(hostAndPort);

        LOG.info("====> TajoWorker:" + workerResource + " added in live TajoWorker list");

        workerResourceLock.notifyAll();
      }
    }
  }
}
