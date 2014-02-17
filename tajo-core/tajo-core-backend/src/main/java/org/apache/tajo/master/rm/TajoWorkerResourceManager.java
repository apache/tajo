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
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.master.querymaster.QueryJobEvent;
import org.apache.tajo.util.ApplicationIdUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TajoWorkerResourceManager implements WorkerResourceManager {
  private static final Log LOG = LogFactory.getLog(TajoWorkerResourceManager.class);

  static AtomicInteger containerIdSeq = new AtomicInteger(0);

  private TajoMaster.MasterContext masterContext;

  //all workers(include querymaster)
  private Map<String, WorkerResource> allWorkerResourceMap = new HashMap<String, WorkerResource>();

  //all workers(include querymaster)
  private Set<String> deadWorkerResources = new HashSet<String>();

  //worker only
  private Set<String> liveWorkerResources = new HashSet<String>();

  //querymaster only
  private Set<String> liveQueryMasterWorkerResources = new HashSet<String>();

  private Map<QueryId, WorkerResource> queryMasterMap = new HashMap<QueryId, WorkerResource>();

  private final Object workerResourceLock = new Object();

  private String queryIdSeed;

  private WorkerResourceAllocationThread workerResourceAllocator;

  private WorkerMonitorThread workerMonitor;

  private BlockingQueue<WorkerResourceRequest> requestQueue;

  private List<WorkerResourceRequest> reAllocationList;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private float queryMasterDefaultDiskSlot;

  private int queryMasterDefaultMemoryMB;

  private TajoConf tajoConf;

  private Map<YarnProtos.ContainerIdProto, AllocatedWorkerResource> allocatedResourceMap =
      new HashMap<YarnProtos.ContainerIdProto, AllocatedWorkerResource>();

  public TajoWorkerResourceManager(TajoMaster.MasterContext masterContext) {
    this.masterContext = masterContext;
    init(masterContext.getConf());
  }

  public TajoWorkerResourceManager(TajoConf tajoConf) {
    init(tajoConf);
  }

  private void init(TajoConf tajoConf) {
    this.tajoConf = tajoConf;
    this.queryIdSeed = String.valueOf(System.currentTimeMillis());

    this.queryMasterDefaultDiskSlot =
        tajoConf.getFloatVar(TajoConf.ConfVars.TAJO_QUERYMASTER_DISK_SLOT);

    this.queryMasterDefaultMemoryMB =
        tajoConf.getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MEMORY_MB);

    requestQueue = new LinkedBlockingDeque<WorkerResourceRequest>();
    reAllocationList = new ArrayList<WorkerResourceRequest>();

    workerResourceAllocator = new WorkerResourceAllocationThread();
    workerResourceAllocator.start();

    workerMonitor = new WorkerMonitorThread();
    workerMonitor.start();
  }

  public Map<String, WorkerResource> getWorkers() {
    return Collections.unmodifiableMap(allWorkerResourceMap);
  }

  public Collection<String> getQueryMasters() {
    return Collections.unmodifiableSet(liveQueryMasterWorkerResources);
  }

  @Override
  public TajoMasterProtocol.ClusterResourceSummary getClusterResourceSummary() {
    int totalDiskSlots = 0;
    int totalCpuCoreSlots = 0;
    int totalMemoryMB = 0;

    int totalAvailableDiskSlots = 0;
    int totalAvailableCpuCoreSlots = 0;
    int totalAvailableMemoryMB = 0;

    synchronized(workerResourceLock) {
      for(String eachWorker: liveWorkerResources) {
        WorkerResource worker = allWorkerResourceMap.get(eachWorker);
        if(worker != null) {
          totalMemoryMB += worker.getMemoryMB();
          totalAvailableMemoryMB += worker.getAvailableMemoryMB();

          totalDiskSlots += worker.getDiskSlots();
          totalAvailableDiskSlots += worker.getAvailableDiskSlots();

          totalCpuCoreSlots += worker.getCpuCoreSlots();
          totalAvailableCpuCoreSlots += worker.getAvailableCpuCoreSlots();
        }
      }
    }

    return TajoMasterProtocol.ClusterResourceSummary.newBuilder()
            .setNumWorkers(liveWorkerResources.size())
            .setTotalCpuCoreSlots(totalCpuCoreSlots)
            .setTotalDiskSlots(totalDiskSlots)
            .setTotalMemoryMB(totalMemoryMB)
            .setTotalAvailableCpuCoreSlots(totalAvailableCpuCoreSlots)
            .setTotalAvailableDiskSlots(totalAvailableDiskSlots)
            .setTotalAvailableMemoryMB(totalAvailableMemoryMB)
            .build();
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
    if(workerMonitor != null) {
      workerMonitor.interrupt();
    }
  }

  @Override
  public WorkerResource allocateQueryMaster(QueryInProgress queryInProgress) {
    return allocateQueryMaster(queryInProgress.getQueryId());
  }

  public WorkerResource allocateQueryMaster(QueryId queryId) {
    synchronized(workerResourceLock) {
      if(liveQueryMasterWorkerResources.size() == 0) {
        LOG.warn("No available resource for querymaster:" + queryId);
        return null;
      }
      WorkerResource queryMasterWorker = null;
      int minTasks = Integer.MAX_VALUE;
      for(String eachQueryMaster: liveQueryMasterWorkerResources) {
        WorkerResource queryMaster = allWorkerResourceMap.get(eachQueryMaster);
        if(queryMaster != null && queryMaster.getNumQueryMasterTasks() < minTasks) {
          queryMasterWorker = queryMaster;
          minTasks = queryMaster.getNumQueryMasterTasks();
        }
      }
      if(queryMasterWorker == null) {
        return null;
      }
      queryMasterWorker.addNumQueryMasterTask(queryMasterDefaultDiskSlot, queryMasterDefaultMemoryMB);
      queryMasterMap.put(queryId, queryMasterWorker);
      LOG.info(queryId + "'s QueryMaster is " + queryMasterWorker);
      return queryMasterWorker;
    }
  }

  @Override
  public void startQueryMaster(QueryInProgress queryInProgress) {
    WorkerResource queryMasterWorkerResource = null;
    synchronized(workerResourceLock) {
      queryMasterWorkerResource = queryMasterMap.get(queryInProgress.getQueryId());
    }

    if(queryMasterWorkerResource != null) {
      AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
      allocatedWorkerResource.workerResource = queryMasterWorkerResource;
      allocatedWorkerResource.allocatedMemoryMB = queryMasterDefaultMemoryMB;
      allocatedWorkerResource.allocatedDiskSlots = queryMasterDefaultDiskSlot;

      startQueryMaster(queryInProgress.getQueryId(), allocatedWorkerResource);
    } else {
      //add queue
      TajoMasterProtocol.WorkerResourceAllocationRequest request =
          TajoMasterProtocol.WorkerResourceAllocationRequest.newBuilder()
            .setExecutionBlockId(QueryIdFactory.newExecutionBlockId(QueryIdFactory.NULL_QUERY_ID, 0).getProto())
            .setNumContainers(1)
            .setMinMemoryMBPerContainer(queryMasterDefaultMemoryMB)
            .setMaxMemoryMBPerContainer(queryMasterDefaultMemoryMB)
            .setMinDiskSlotPerContainer(queryMasterDefaultDiskSlot)
            .setMaxDiskSlotPerContainer(queryMasterDefaultDiskSlot)
            .setResourceRequestPriority(TajoMasterProtocol.ResourceRequestPriority.MEMORY)
            .build();
      try {
        requestQueue.put(new WorkerResourceRequest(queryInProgress.getQueryId(), true, request, null));
      } catch (InterruptedException e) {
      }
    }
  }

  private void startQueryMaster(QueryId queryId, AllocatedWorkerResource workResource) {
    QueryInProgress queryInProgress = masterContext.getQueryJobManager().getQueryInProgress(queryId);
    if(queryInProgress == null) {
      LOG.warn("No QueryInProgress while starting  QueryMaster:" + queryId);
      return;
    }
    queryInProgress.getQueryInfo().setQueryMasterResource(workResource.workerResource);

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

  class WorkerMonitorThread extends Thread {
    int heartbeatTimeout;

    @Override
    public void run() {
      heartbeatTimeout = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_HEARTBEAT_TIMEOUT);
      LOG.info("WorkerMonitor start");
      while(!stopped.get()) {
        try {
          Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
          if(stopped.get()) {
            break;
          }
        }
        synchronized(workerResourceLock) {
          Set<String> workerHolders = new HashSet<String>();
          workerHolders.addAll(liveWorkerResources);
          for(String eachLiveWorker: workerHolders) {
            WorkerResource worker = allWorkerResourceMap.get(eachLiveWorker);
            if(worker == null) {
              LOG.warn(eachLiveWorker + " not in WorkerReosurceMap");
              continue;
            }

            if(System.currentTimeMillis() - worker.getLastHeartbeat() >= heartbeatTimeout) {
              liveWorkerResources.remove(eachLiveWorker);
              deadWorkerResources.add(eachLiveWorker);
              worker.setWorkerStatus(WorkerStatus.DEAD);
              LOG.warn("Worker [" + eachLiveWorker + "] is dead.");
            }
          }

          //QueryMaster
          workerHolders.clear();

          workerHolders.addAll(liveQueryMasterWorkerResources);
          for(String eachLiveWorker: workerHolders) {
            WorkerResource worker = allWorkerResourceMap.get(eachLiveWorker);
            if(worker == null) {
              LOG.warn(eachLiveWorker + " not in WorkerResourceMap");
              continue;
            }

            if(System.currentTimeMillis() - worker.getLastHeartbeat() >= heartbeatTimeout) {
              liveQueryMasterWorkerResources.remove(eachLiveWorker);
              deadWorkerResources.add(eachLiveWorker);
              worker.setWorkerStatus(WorkerStatus.DEAD);
              LOG.warn("QueryMaster [" + eachLiveWorker + "] is dead.");
            }
          }
        }
      }
    }
  }

  static class WorkerResourceRequest {
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

  static class AllocatedWorkerResource {
    WorkerResource workerResource;
    int allocatedMemoryMB;
    float allocatedDiskSlots;
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
                ", requiredMemory:" + resourceRequest.request.getMinMemoryMBPerContainer() +
                "~" + resourceRequest.request.getMaxMemoryMBPerContainer() +
                ", requiredContainers:" + resourceRequest.request.getNumContainers() +
                ", requiredDiskSlots:" + resourceRequest.request.getMinDiskSlotPerContainer() +
                "~" + resourceRequest.request.getMaxDiskSlotPerContainer() +
                ", queryMasterRequest=" + resourceRequest.queryMasterRequest +
                ", liveWorkers=" + liveWorkerResources.size());
          }

          List<AllocatedWorkerResource> allocatedWorkerResources = chooseWorkers(resourceRequest);

          if(allocatedWorkerResources.size() > 0) {
            if(resourceRequest.queryMasterRequest) {
              startQueryMaster(resourceRequest.queryId, allocatedWorkerResources.get(0));
            } else {
              List<TajoMasterProtocol.WorkerAllocatedResource> allocatedResources =
                  new ArrayList<TajoMasterProtocol.WorkerAllocatedResource>();

              for(AllocatedWorkerResource eachWorker: allocatedWorkerResources) {
                NodeId nodeId = NodeId.newInstance(eachWorker.workerResource.getAllocatedHost(),
                    eachWorker.workerResource.getPeerRpcPort());

                TajoWorkerContainerId containerId = new TajoWorkerContainerId();

                containerId.setApplicationAttemptId(
                    ApplicationIdUtils.createApplicationAttemptId(resourceRequest.queryId));
                containerId.setId(containerIdSeq.incrementAndGet());

                YarnProtos.ContainerIdProto containerIdProto = containerId.getProto();
                allocatedResources.add(TajoMasterProtocol.WorkerAllocatedResource.newBuilder()
                    .setContainerId(containerIdProto)
                    .setNodeId(nodeId.toString())
                    .setWorkerHost(eachWorker.workerResource.getAllocatedHost())
                    .setQueryMasterPort(eachWorker.workerResource.getQueryMasterPort())
                    .setPeerRpcPort(eachWorker.workerResource.getPeerRpcPort())
                    .setWorkerPullServerPort(eachWorker.workerResource.getPullServerPort())
                    .setAllocatedMemoryMB(eachWorker.allocatedMemoryMB)
                    .setAllocatedDiskSlots(eachWorker.allocatedDiskSlots)
                    .build());

                synchronized(workerResourceLock) {
                  allocatedResourceMap.put(containerIdProto, eachWorker);
                }
              }

              resourceRequest.callBack.run(TajoMasterProtocol.WorkerResourceAllocationResponse.newBuilder()
                  .setExecutionBlockId(resourceRequest.request.getExecutionBlockId())
                  .addAllWorkerAllocatedResource(allocatedResources)
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

  private List<AllocatedWorkerResource> chooseWorkers(WorkerResourceRequest resourceRequest) {
    List<AllocatedWorkerResource> selectedWorkers = new ArrayList<AllocatedWorkerResource>();

    int allocatedResources = 0;

    if(resourceRequest.queryMasterRequest) {
      WorkerResource worker = allocateQueryMaster(resourceRequest.queryId);
      if(worker != null) {
        AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
        allocatedWorkerResource.workerResource = worker;
        allocatedWorkerResource.allocatedDiskSlots = queryMasterDefaultDiskSlot;
        allocatedWorkerResource.allocatedMemoryMB = queryMasterDefaultMemoryMB;
        selectedWorkers.add(allocatedWorkerResource);

        return selectedWorkers;
      }
    }

    TajoMasterProtocol.ResourceRequestPriority resourceRequestPriority
        = resourceRequest.request.getResourceRequestPriority();

    if(resourceRequestPriority == TajoMasterProtocol.ResourceRequestPriority.MEMORY) {
      synchronized(workerResourceLock) {
        List<String> randomWorkers = new ArrayList<String>(liveWorkerResources);
        Collections.shuffle(randomWorkers);

        int numContainers = resourceRequest.request.getNumContainers();
        int minMemoryMB = resourceRequest.request.getMinMemoryMBPerContainer();
        int maxMemoryMB = resourceRequest.request.getMaxMemoryMBPerContainer();
        float diskSlot = Math.max(resourceRequest.request.getMaxDiskSlotPerContainer(),
            resourceRequest.request.getMinDiskSlotPerContainer());

        int liveWorkerSize = randomWorkers.size();
        Set<String> insufficientWorkers = new HashSet<String>();
        boolean stop = false;
        boolean checkMax = true;
        while(!stop) {
          if(allocatedResources >= numContainers) {
            break;
          }

          if(insufficientWorkers.size() >= liveWorkerSize) {
            if(!checkMax) {
              break;
            }
            insufficientWorkers.clear();
            checkMax = false;
          }
          int compareAvailableMemory = checkMax ? maxMemoryMB : minMemoryMB;

          for(String eachWorker: randomWorkers) {
            if(allocatedResources >= numContainers) {
              stop = true;
              break;
            }

            if(insufficientWorkers.size() >= liveWorkerSize) {
              break;
            }

            WorkerResource workerResource = allWorkerResourceMap.get(eachWorker);
            if(workerResource.getAvailableMemoryMB() >= compareAvailableMemory) {
              int workerMemory;
              if(workerResource.getAvailableMemoryMB() >= maxMemoryMB) {
                workerMemory = maxMemoryMB;
              } else {
                workerMemory = workerResource.getAvailableMemoryMB();
              }
              AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
              allocatedWorkerResource.workerResource = workerResource;
              allocatedWorkerResource.allocatedMemoryMB = workerMemory;
              if(workerResource.getAvailableDiskSlots() >= diskSlot) {
                allocatedWorkerResource.allocatedDiskSlots = diskSlot;
              } else {
                allocatedWorkerResource.allocatedDiskSlots = workerResource.getAvailableDiskSlots();
              }

              workerResource.allocateResource(allocatedWorkerResource.allocatedDiskSlots,
                  allocatedWorkerResource.allocatedMemoryMB);

              selectedWorkers.add(allocatedWorkerResource);

              allocatedResources++;
            } else {
              insufficientWorkers.add(eachWorker);
            }
          }
        }
      }
    } else {
      synchronized(workerResourceLock) {
        List<String> randomWorkers = new ArrayList<String>(liveWorkerResources);
        Collections.shuffle(randomWorkers);

        int numContainers = resourceRequest.request.getNumContainers();
        float minDiskSlots = resourceRequest.request.getMinDiskSlotPerContainer();
        float maxDiskSlots = resourceRequest.request.getMaxDiskSlotPerContainer();
        int memoryMB = Math.max(resourceRequest.request.getMaxMemoryMBPerContainer(),
            resourceRequest.request.getMinMemoryMBPerContainer());

        int liveWorkerSize = randomWorkers.size();
        Set<String> insufficientWorkers = new HashSet<String>();
        boolean stop = false;
        boolean checkMax = true;
        while(!stop) {
          if(allocatedResources >= numContainers) {
            break;
          }

          if(insufficientWorkers.size() >= liveWorkerSize) {
            if(!checkMax) {
              break;
            }
            insufficientWorkers.clear();
            checkMax = false;
          }
          float compareAvailableDisk = checkMax ? maxDiskSlots : minDiskSlots;

          for(String eachWorker: randomWorkers) {
            if(allocatedResources >= numContainers) {
              stop = true;
              break;
            }

            if(insufficientWorkers.size() >= liveWorkerSize) {
              break;
            }

            WorkerResource workerResource = allWorkerResourceMap.get(eachWorker);
            if(workerResource.getAvailableDiskSlots() >= compareAvailableDisk) {
              float workerDiskSlots;
              if(workerResource.getAvailableDiskSlots() >= maxDiskSlots) {
                workerDiskSlots = maxDiskSlots;
              } else {
                workerDiskSlots = workerResource.getAvailableDiskSlots();
              }
              AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
              allocatedWorkerResource.workerResource = workerResource;
              allocatedWorkerResource.allocatedDiskSlots = workerDiskSlots;

              if(workerResource.getAvailableMemoryMB() >= memoryMB) {
                allocatedWorkerResource.allocatedMemoryMB = memoryMB;
              } else {
                allocatedWorkerResource.allocatedMemoryMB = workerResource.getAvailableMemoryMB();
              }
              workerResource.allocateResource(allocatedWorkerResource.allocatedDiskSlots,
                  allocatedWorkerResource.allocatedMemoryMB);

              selectedWorkers.add(allocatedWorkerResource);

              allocatedResources++;
            } else {
              insufficientWorkers.add(eachWorker);
            }
          }
        }
      }
    }
    return selectedWorkers;
  }

  @Override
  public void releaseWorkerResource(ExecutionBlockId ebId, YarnProtos.ContainerIdProto containerId) {
    synchronized(workerResourceLock) {
      AllocatedWorkerResource allocatedWorkerResource = allocatedResourceMap.get(containerId);
      if(allocatedWorkerResource != null) {
        LOG.info("Release Resource:" + ebId + "," +
            allocatedWorkerResource.allocatedDiskSlots + "," + allocatedWorkerResource.allocatedMemoryMB);
        allocatedWorkerResource.workerResource.releaseResource(
            allocatedWorkerResource.allocatedDiskSlots, allocatedWorkerResource.allocatedMemoryMB);
      } else {
        LOG.warn("No AllocatedWorkerResource data for [" + ebId + "," + containerId + "]");
        return;
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
        queryMasterWorkerResource.releaseQueryMasterTask(queryMasterDefaultDiskSlot, queryMasterDefaultMemoryMB);
      }
    }

    LOG.info("release QueryMaster resource:" + queryId + "," + queryMasterWorkerResource);
  }

  public void workerHeartbeat(TajoMasterProtocol.TajoHeartbeat request) {
    synchronized(workerResourceLock) {
      String workerKey = request.getTajoWorkerHost() + ":" + request.getTajoQueryMasterPort() + ":"
          + request.getPeerRpcPort();
      boolean queryMasterMode = request.getServerStatus().getQueryMasterMode().getValue();
      boolean taskRunnerMode = request.getServerStatus().getTaskRunnerMode().getValue();

      if(allWorkerResourceMap.containsKey(workerKey)) {
        WorkerResource workerResource = allWorkerResourceMap.get(workerKey);

        if(deadWorkerResources.contains(workerKey)) {
          deadWorkerResources.remove(workerKey);
          if(queryMasterMode) {
            liveQueryMasterWorkerResources.add(workerKey);
            workerResource.setNumRunningTasks(0);
            LOG.info("Heartbeat received from QueryMaster [" + workerKey + "] again.");
          }
          if(taskRunnerMode) {
            liveWorkerResources.add(workerKey);
            LOG.info("Heartbeat received from Worker [" + workerKey + "] again.");
          }
        }
        workerResource.setLastHeartbeat(System.currentTimeMillis());
        workerResource.setWorkerStatus(WorkerStatus.LIVE);
        workerResource.setNumRunningTasks(request.getServerStatus().getRunningTaskNum());
        workerResource.setMaxHeap(request.getServerStatus().getJvmHeap().getMaxHeap());
        workerResource.setFreeHeap(request.getServerStatus().getJvmHeap().getFreeHeap());
        workerResource.setTotalHeap(request.getServerStatus().getJvmHeap().getTotalHeap());
      } else {
        //initial connection
        WorkerResource workerResource = new WorkerResource();
        workerResource.setAllocatedHost(request.getTajoWorkerHost());
        workerResource.setQueryMasterMode(queryMasterMode);
        workerResource.setTaskRunnerMode(taskRunnerMode);

        workerResource.setQueryMasterPort(request.getTajoQueryMasterPort());
        workerResource.setPeerRpcPort(request.getPeerRpcPort());
        workerResource.setClientPort(request.getTajoWorkerClientPort());
        workerResource.setPullServerPort(request.getTajoWorkerPullServerPort());
        workerResource.setHttpPort(request.getTajoWorkerHttpPort());

        workerResource.setLastHeartbeat(System.currentTimeMillis());
        workerResource.setWorkerStatus(WorkerStatus.LIVE);
        if(request.getServerStatus() != null) {
          workerResource.setMemoryMB(request.getServerStatus().getMemoryResourceMB());
          workerResource.setCpuCoreSlots(request.getServerStatus().getSystem().getAvailableProcessors());
          workerResource.setDiskSlots(request.getServerStatus().getDiskSlots());
          workerResource.setNumRunningTasks(request.getServerStatus().getRunningTaskNum());
          workerResource.setMaxHeap(request.getServerStatus().getJvmHeap().getMaxHeap());
          workerResource.setFreeHeap(request.getServerStatus().getJvmHeap().getFreeHeap());
          workerResource.setTotalHeap(request.getServerStatus().getJvmHeap().getTotalHeap());
        } else {
          workerResource.setMemoryMB(4096);
          workerResource.setDiskSlots(4);
          workerResource.setCpuCoreSlots(4);
        }

        allWorkerResourceMap.put(workerResource.getId(), workerResource);
        if(queryMasterMode) {
          liveQueryMasterWorkerResources.add(workerKey);
        }

        if(taskRunnerMode) {
          liveWorkerResources.add(workerKey);
        }

        LOG.info("TajoWorker:" + workerResource + " added in live TajoWorker list");

        workerResourceLock.notifyAll();
      }
    }
  }
}
