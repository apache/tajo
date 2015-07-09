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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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

  private static final Comparator<QuerySchedulingInfo> COMPARATOR = new SchedulingAlgorithms.FifoComparator();

  private volatile boolean isStopped = false;
  private final TajoMaster.MasterContext masterContext;

  private final TajoRMContext rmContext;
  private final BlockingQueue<QuerySchedulingInfo> queryQueue;
  private final Map<QueryId, QuerySchedulingInfo> pendingQueryMap = Maps.newHashMap();

  private final Map<QueryId, Integer> assignedQueryMasterMap = Maps.newHashMap();
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  private final Thread queryProcessor;
  private TajoConf tajoConf;

  @VisibleForTesting
  public SimpleScheduler(TajoMaster.MasterContext context, TajoRMContext rmContext) {
    super(SimpleScheduler.class.getName());
    this.masterContext = context;
    this.rmContext = rmContext;
    //Copy default array capacity from PriorityBlockingQueue.
    this.queryQueue = new PriorityBlockingQueue<QuerySchedulingInfo>(11, COMPARATOR);
    this.queryProcessor = new Thread(new QueryProcessor());
  }

  public SimpleScheduler(TajoMaster.MasterContext context) {
    this(context, context.getResourceManager().getRMContext());
  }

  private void initScheduler(TajoConf conf) {
    int minQMMem = conf.getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MINIMUM_MEMORY);
    this.minResource.setMemory(minQMMem).setVirtualCores(1);
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

    if(LOG.isDebugEnabled()) {
      LOG.debug("Cluster Resource. available : " + getClusterResource()
          + " maximum: " + getMaximumResourceCapability());
    }
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
  protected void serviceStop() throws Exception {
    this.isStopped = true;
    super.serviceStop();
  }

  @Override
  public int getRunningQuery() {
    return assignedQueryMasterMap.size();
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  private QueryCoordinatorProtocol.NodeResourceRequestProto createQMResourceRequest(QueryInfo queryInfo) {
    int qmMemory = tajoConf.getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MINIMUM_MEMORY);
    NodeResource qmResource = NodeResources.createResource(qmMemory);


    int containers = 1;
    Set<Integer> assignedQMNodes = Sets.newHashSet(assignedQueryMasterMap.values());
    List<Integer> idleNode = Lists.newArrayList();

    for (Worker worker : getRMContext().getWorkers().values()) {

      //find idle node for QM
      if (!assignedQMNodes.contains(worker.getWorkerId())) {
        idleNode.add(worker.getWorkerId());
      }

      if (idleNode.size() > containers * 3) break;
    }

    QueryCoordinatorProtocol.NodeResourceRequestProto.Builder builder =
        QueryCoordinatorProtocol.NodeResourceRequestProto.newBuilder();

    builder.setQueryId(queryInfo.getQueryId().getProto())
        .setCapacity(qmResource.getProto())
        .setType(QueryCoordinatorProtocol.ResourceType.QUERYMASTER)
        .setPriority(1)
        .setNumContainers(containers)
        .setRunningTasks(1)
        .addAllCandidateNodes(idleNode)
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

    List<AllocationResourceProto> reservedResources;
    NodeResource capacity = new NodeResource(request.getCapacity());
    if (!NodeResources.fitsIn(capacity, getClusterResource())) {
      return Lists.newArrayList();
    }

    LinkedList<Integer> workers = new LinkedList<Integer>();

    if (request.getCandidateNodesCount() > 0) {
      workers.addAll(request.getCandidateNodesList());
      Collections.shuffle(workers);
    }

    int requiredContainers = request.getNumContainers();
    // reserve resource from candidate workers for locality
    reservedResources = reserveClusterResource(workers, capacity, requiredContainers);

    // reserve resource in random workers
    if (reservedResources.size() < requiredContainers) {
      LinkedList<Integer> randomWorkers = new LinkedList<Integer>(getRMContext().getWorkers().keySet());
      Collections.shuffle(randomWorkers);

      reservedResources.addAll(reserveClusterResource(
          randomWorkers, capacity, requiredContainers - reservedResources.size()));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Request: " + request.getCapacity() + ", containerNum:" + request.getNumContainers()
          + "Current cluster resource: " + getClusterResource());
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
        //TODO should consider request priority
        reserveResource(TUtil.checkTypeAndGet(event, ResourceReserveSchedulerEvent.class));
        break;
      case RESOURCE_UPDATE:
        updateResource();
        break;
      default:
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

  protected boolean startQuery(QueryId queryId, AllocationResourceProto allocation) {
   return masterContext.getQueryJobManager().startQueryJob(queryId, allocation);
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

  protected QueryInfo getQueryInfo(QueryId queryId) {
    return masterContext.getQueryJobManager().getQueryInProgress(queryId).getQueryInfo();
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
        //TODO get by assigned queue
        int maxAvailable = getResourceCalculator().computeAvailableContainers(
            getMaximumResourceCapability(), getMinimumResourceCapability());

        // check maximum running queries
        if (assignedQueryMasterMap.size() * 2 > maxAvailable) {
          queryQueue.add(query);
          synchronized (this) {
            try {
              this.wait(1000);
            } catch (InterruptedException e) {
              if(!isStopped) {
                LOG.fatal(e.getMessage(), e);
                return;
              }
            }
          }
        } else {
          QueryInfo queryInfo = getQueryInfo(query.getQueryId());
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
              //if QM resource can't be allocated to a node, it should retry
              boolean started = startQuery(query.getQueryId(), allocation.get(0));
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
}
