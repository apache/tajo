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

package org.apache.tajo.master.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.rm.TajoRMContext;
import org.apache.tajo.master.rm.Worker;
import org.apache.tajo.master.scheduler.event.ResourceReserveSchedulerEvent;
import org.apache.tajo.master.scheduler.event.SchedulerEvent;
import org.apache.tajo.resource.DefaultResourceCalculator;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.resource.ResourceCalculator;
import org.apache.tajo.util.TUtil;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import static org.apache.tajo.ipc.QueryCoordinatorProtocol.AllocationResourceProto;

public class SimpleScheduler extends AbstractQueryScheduler {

  private static final Log LOG = LogFactory.getLog(SimpleScheduler.class);

  private static final String DEFAULT_QUEUE_NAME = "default";
  private static final Comparator<QuerySchedulingInfo> COMPARATOR = new SchedulingAlgorithms.FifoComparator();

  private volatile boolean isStopped = false;
  private final TajoMaster.MasterContext masterContext;

  private final TajoRMContext rmContext;
  private final BlockingQueue<QuerySchedulingInfo> queryQueue;
  private final Map<QueryId, QuerySchedulingInfo> pendingQueryMap = Maps.newHashMap();

  private final Map<QueryId, Integer> assignedQueryMasterMap = Maps.newHashMap();
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();


  private final Thread queryProcessor;
  private QueueInfo queueInfo;

  private TajoConf tajoConf;

  public SimpleScheduler(TajoMaster.MasterContext context) {
    super(SimpleScheduler.class.getName());
    this.masterContext = context;
    this.rmContext = context.getResourceManager().getRMContext();
    this.queueInfo = new SimpleQueue();
    this.queryQueue = new PriorityBlockingQueue<QuerySchedulingInfo>(11, COMPARATOR);
    this.queryProcessor = new Thread(new QueryProcessor());
  }

  private void initScheduler(TajoConf conf) {
    validateConf(conf);
    int minQMMem = conf.getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MINIMUM_MEMORY);
    this.minResource.setMemory(minQMMem).setVirtualCores(1);
    this.queueInfo.setCapacity(1.0f);
    this.queueInfo.setMaximumCapacity(queueInfo.getCapacity());
    this.queueInfo.setMaximumQueryCapacity(0.3f); // maximum parall
    this.queueInfo.setQueueState(QueueState.RUNNING);
    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());
    updateResource();
    this.queryProcessor.setName("Query Processor");
  }

  private void updateResource() {
    NodeResource resource = NodeResources.createResource(0);
    NodeResource totalResource = NodeResources.createResource(0);
    for (Worker worker : getRMContext().getWorkers().values()) {
      NodeResources.addTo(resource, worker.getAvailableResource());
      NodeResources.addTo(totalResource, worker.getTotalResourceCapability());

    }

    NodeResources.update(maxResource, totalResource);
    NodeResources.update(clusterResource, resource);

    if (getResourceCalculator().isInvalidDivisor(clusterResource)) {
      this.queueInfo.setCurrentCapacity(0.0f);
    } else {
      this.queueInfo.setCurrentCapacity(getResourceCalculator().ratio(clusterResource, maxResource));
    }

    LOG.info("Scheduler resources \n current: " + getClusterResource()
        + "\n maximum: " + getMaximumResourceCapability() + "\n queue: " + queueInfo);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.tajoConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    initScheduler(tajoConf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    this.queryProcessor.start();
    super.serviceStart();
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  private void validateConf(TajoConf conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getIntVar(TajoConf.ConfVars.TASK_RESOURCE_MINIMUM_MEMORY);
    int minQMMem = conf.getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MINIMUM_MEMORY);
    int maxMem = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB);

    if (minMem <= 0 || minQMMem <= 0 || minMem + minQMMem > maxMem) {
      throw new RuntimeException("Invalid resource scheduler memory"
          + " allocation configuration"
          + ", " + TajoConf.ConfVars.TASK_RESOURCE_MINIMUM_MEMORY.varname
          + "=" + minMem
          + ", " + TajoConf.ConfVars.TAJO_QUERYMASTER_MINIMUM_MEMORY.varname
          + "=" + minQMMem
          + ", " + TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB.varname
          + "=" + maxMem + ", min and max should be greater than 0"
          + ", max should be no smaller than min.");
    }
  }

  private QueryCoordinatorProtocol.NodeResourceRequestProto createQMResourceRequest(QueryInfo queryInfo) {
    int qmMemory = tajoConf.getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MINIMUM_MEMORY);

    QueryCoordinatorProtocol.NodeResourceRequestProto.Builder builder =
        QueryCoordinatorProtocol.NodeResourceRequestProto.newBuilder();

    builder.setQueryId(queryInfo.getQueryId().getProto())
        .setCapacity(NodeResources.createResource(qmMemory).getProto())
        .setType(QueryCoordinatorProtocol.ResourceType.QUERYMASTER)
        .setPriority(1)
        .setNumContainers(1)
        .setRunningTasks(1)
        .setUserId(queryInfo.getQueryContext().getUser());
    //TODO .setQueue(queryInfo.getQueue());
    return builder.build();
  }


  @Override
  public int getNumClusterNodes() {
    return rmContext.getWorkers().size();
  }

  @Override
  public List<AllocationResourceProto>
  reserve(QueryId queryId, QueryCoordinatorProtocol.NodeResourceRequestProto request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Request:" + request.toString() + "Cluster resource: " + getClusterResource());
    }

    List<AllocationResourceProto> reservedResources;
    NodeResource capacity = new NodeResource(request.getCapacity());
    if (!NodeResources.fitsIn(capacity, getClusterResource())) {
      return Lists.newArrayList();
    }

    LinkedList<Integer> workers = new LinkedList<Integer>();

    if (request.getCandidateNodesCount() > 0) {
      workers.addAll(request.getCandidateNodesList());
    }

    int requiredContainers = request.getNumContainers();
    // reserve resource to the candidate workers for locality
    reservedResources = reserveClusterResource(workers, capacity, requiredContainers);

    // reserve resource in random workers
    if (reservedResources.size() < requiredContainers) {
      LinkedList<Integer> randomWorkers = new LinkedList<Integer>(getRMContext().getWorkers().keySet());
      randomWorkers.removeAll(workers);
      Collections.shuffle(randomWorkers);

      reservedResources.addAll(reserveClusterResource(
          randomWorkers, capacity, requiredContainers - reservedResources.size()));
    }

    return reservedResources;
  }

  private List<AllocationResourceProto> reserveClusterResource(List<Integer> workers,
                                                               NodeResource capacity, int requiredNum) {

    List<AllocationResourceProto> reservedResources = Lists.newArrayList();
    AllocationResourceProto.Builder resourceBuilder = AllocationResourceProto.newBuilder();
    int allocatedResources = 0;

    while (workers.size() > 0) {
      Iterator<Integer> iter = workers.iterator();
      while (iter.hasNext()) {

        int workerId = iter.next();
        Worker worker = getRMContext().getWorkers().get(workerId);
        if (worker == null) {
          iter.remove();
          LOG.warn("Can't found the worker :" + workerId);
          continue;
        } else {
          if (NodeResources.fitsIn(capacity, worker.getAvailableResource())) {
            NodeResources.subtractFrom(getClusterResource(), capacity);
            NodeResources.subtractFrom(worker.getAvailableResource(), capacity);
            allocatedResources++;
            resourceBuilder.setResource(capacity.getProto());
            resourceBuilder.setWorkerId(workerId);
            reservedResources.add(resourceBuilder.build());
          } else {
            // remove unavailable worker;
            iter.remove();
          }
        }

        if (allocatedResources >= requiredNum) {
          return reservedResources;
        }
      }
    }
    return reservedResources;
  }


  @Override
  public void handle(SchedulerEvent event) {
    switch (event.getType()) {
      case RESOURCE_RESERVE:
        reserveResource(TUtil.checkTypeAndGet(event, ResourceReserveSchedulerEvent.class));
        break;
      case RESOURCE_UPDATE:
        updateResource();
        break;
    }
  }

  /**
   * This is an asynchronous call. You should use a callback to get reserved resource containers.
   */
  protected void reserveResource(ResourceReserveSchedulerEvent schedulerEvent) {
    List<AllocationResourceProto> resources =
        reserve(new QueryId(schedulerEvent.getRequest().getQueryId()), schedulerEvent.getRequest());

    QueryCoordinatorProtocol.NodeResourceResponseProto.Builder response =
        QueryCoordinatorProtocol.NodeResourceResponseProto.newBuilder();
    response.setQueryId(schedulerEvent.getRequest().getQueryId());
    schedulerEvent.getCallBack().run(response.addAllResource(resources).build());
  }

  /**
   * Submit a query to scheduler
   */
  public void submitQuery(QuerySchedulingInfo schedulingInfo) {
    queryQueue.add(schedulingInfo);
    pendingQueryMap.put(schedulingInfo.getQueryId(), schedulingInfo);
  }

  public void stopQuery(QueryId queryId) {
    if(pendingQueryMap.containsKey(queryId)){
      queryQueue.remove(pendingQueryMap.remove(queryId));
    }
    assignedQueryMasterMap.remove(queryId);
  }

  public BlockingQueue<QuerySchedulingInfo> getQueryQueue() {
    return queryQueue;
  }

  private Worker getWorker(int workerId) {
    return rmContext.getWorkers().get(workerId);
  }

  protected TajoRMContext getRMContext() {
    return rmContext;
  }

  public WorkerConnectionInfo getQueryMaster(QueryId queryId) {
    if (assignedQueryMasterMap.containsKey(queryId)) {
      return rmContext.getWorkers().get(assignedQueryMasterMap.get(queryId)).getConnectionInfo();
    }
    return null;
  }

  private final class QueryProcessor implements Runnable {
    @Override
    public void run() {

      QuerySchedulingInfo query;

      while (!isStopped && !Thread.currentThread().isInterrupted()) {
        try {
          query = queryQueue.take();
        } catch (InterruptedException e) {
          e.printStackTrace();
          break;
        }

        //QueueInfo queueInfo = getQueueInfo(query.getQueue(), true, true);

        int maxAvailable = getResourceCalculator().computeAvailableContainers(
            getMaximumResourceCapability(), getMinimumResourceCapability());

        // limit maximum running queries
//        if ((assignedQueryMasterMap.size() / maxAvailable) > queueInfo.getCurrentCapacity()) {
        if (assignedQueryMasterMap.size() * 2 > maxAvailable) {
          queryQueue.add(query);
          synchronized (this) {
            try {
              this.wait(100);
            } catch (InterruptedException e) {
              if(!isStopped) {
                LOG.fatal(e.getMessage(), e);
                return;
              }
            }
          }
        } else {
          QueryInfo queryInfo =
              masterContext.getQueryJobManager().getQueryInProgress(query.getQueryId()).getQueryInfo();
          List<QueryCoordinatorProtocol.AllocationResourceProto> allocation =
              reserve(query.getQueryId(), createQMResourceRequest(queryInfo));

          if(allocation.size() == 0) {
            queryQueue.add(query);
            synchronized (this) {
              try {
                this.wait(100);
              } catch (InterruptedException e) {
                LOG.fatal(e);
              }
            }
            LOG.info("No Available Resources for QueryMaster :" + queryInfo.getQueryId() + "," + queryInfo);
          } else {
            try {
              boolean started = masterContext.getQueryJobManager().startQueryJob(query.getQueryId(), allocation.get(0));
              if(!started) {
                queryQueue.put(query);
              } else {
                assignedQueryMasterMap.put(query.getQueryId(), allocation.get(0).getWorkerId());
              }
            } catch (Throwable t) {
              LOG.fatal("Exception during query startup:", t);
              masterContext.getQueryJobManager().stopQuery(query.getQueryId());
            }
          }
        }
        LOG.info("Running Queries: " + assignedQueryMasterMap.size());
      }
    }
  }

  static class SimpleQueue extends QueueInfo {
    private List<QueueInfo> childQueues;
    private float capacity;
    private float currentCapacity;
    private float maximumCapacity;
    private float maximumQueryCapacity;
    private QueueState state;

    @Override
    public String getQueueName() {
      return DEFAULT_QUEUE_NAME;
    }

    @Override
    public void setQueueName(String queueName) {
    }

    @Override
    public float getCapacity() {
      return capacity;
    }

    @Override
    public void setCapacity(float capacity) {
      this.capacity = capacity;
    }

    @Override
    public float getMaximumCapacity() {
      return maximumCapacity;
    }

    @Override
    public void setMaximumCapacity(float maximumCapacity) {
      this.maximumCapacity = maximumCapacity;
    }

    @Override
    public float getMaximumQueryCapacity() {
      return maximumQueryCapacity;
    }

    @Override
    public void setMaximumQueryCapacity(float maximumQueryCapacity) {
      this.maximumQueryCapacity = maximumQueryCapacity;
    }

    @Override
    public float getCurrentCapacity() {
      return currentCapacity;
    }

    @Override
    public void setCurrentCapacity(float currentCapacity) {
      this.currentCapacity = currentCapacity;
    }

    @Override
    public List<QueueInfo> getChildQueues() {
      return childQueues;
    }

    @Override
    public void setChildQueues(List<QueueInfo> childQueues) {
       this.childQueues = childQueues;
    }

    @Override
    public QueueState getQueueState() {
      return state;
    }

    @Override
    public void setQueueState(QueueState queueState) {
      this.state = queueState;
    }

    @Override
    public String toString() {
      return String.format("Queue name: %s, state: %s, maximum: %f, current: %f",
          getQueueName(), getQueueState(), getMaximumCapacity(), getCurrentCapacity());
    }
  }
}
