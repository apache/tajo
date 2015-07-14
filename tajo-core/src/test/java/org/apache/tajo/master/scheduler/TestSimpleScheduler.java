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
import net.jcip.annotations.NotThreadSafe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.rm.*;
import org.apache.tajo.master.scheduler.event.ResourceReserveSchedulerEvent;
import org.apache.tajo.master.scheduler.event.SchedulerEvent;
import org.apache.tajo.master.scheduler.event.SchedulerEventType;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.rpc.CallFuture;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.tajo.ipc.QueryCoordinatorProtocol.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@NotThreadSafe
public class TestSimpleScheduler {
  private CompositeService service;
  private SimpleScheduler scheduler;
  private TajoRMContext rmContext;
  private AsyncDispatcher dispatcher;
  private TajoConf conf;
  private int workerNum = 3;
  private NodeResource nodeResource;
  private NodeResource totalResource;
  private Semaphore barrier;
  private int testDelay = 50;
  private static ScheduledExecutorService executorService;

  @BeforeClass
  public static void setupClass() {
    executorService = Executors.newScheduledThreadPool(10);
  }

  @AfterClass
  public static void tearDownClass() {
    executorService.shutdown();
  }

  @Before
  public void setup() {
    conf = new TajoConf();
    nodeResource = NodeResource.createResource(1500, 2, 3);
    service = new CompositeService(TestSimpleScheduler.class.getSimpleName()) {

      @Override
      protected void serviceInit(Configuration conf) throws Exception {
        dispatcher = new AsyncDispatcher();
        addService(dispatcher);

        rmContext = new TajoRMContext(dispatcher);
        rmContext.getDispatcher().register(WorkerEventType.class,
            new TajoResourceManager.WorkerEventDispatcher(rmContext));

        barrier = new Semaphore(0);
        scheduler = new MySimpleScheduler(rmContext, barrier);
        addService(scheduler);
        rmContext.getDispatcher().register(SchedulerEventType.class, scheduler);

        for (int i = 0; i < workerNum; i++) {
          WorkerConnectionInfo conn = new WorkerConnectionInfo("host" + i, 28091 + i, 28092, 21000, 28093, 28080);
          rmContext.getWorkers().putIfAbsent(conn.getId(),
              new Worker(rmContext, NodeResources.clone(nodeResource), conn));
          rmContext.getDispatcher().getEventHandler().handle(new WorkerEvent(conn.getId(), WorkerEventType.STARTED));
        }
        super.serviceInit(conf);
      }
    };
    service.init(conf);
    service.start();

    assertEquals(workerNum, rmContext.getWorkers().size());
    totalResource = NodeResources.createResource(0);
    for(Worker worker : rmContext.getWorkers().values()) {
      NodeResources.addTo(totalResource, worker.getTotalResourceCapability());
    }
  }

  @After
  public void tearDown() {
    service.stop();
  }

  @Test
  public void testInitialCapacity() throws InterruptedException {
    assertEquals(workerNum, scheduler.getNumClusterNodes());
    assertEquals(0, scheduler.getRunningQuery());

    assertEquals(totalResource, scheduler.getMaximumResourceCapability());
    assertEquals(totalResource, scheduler.getClusterResource());

    assertEquals(TajoConf.ConfVars.QUERYMASTER_MINIMUM_MEMORY.defaultIntVal,
        scheduler.getQMMinimumResourceCapability().getMemory());

    assertEquals(TajoConf.ConfVars.TASK_RESOURCE_MINIMUM_MEMORY.defaultIntVal,
        scheduler.getMinimumResourceCapability().getMemory());
  }

  @Test(timeout = 10000)
  public void testSubmitOneQuery() throws InterruptedException {
    QuerySchedulingInfo schedulingInfo = new QuerySchedulingInfo("default",
        "user",
        QueryIdFactory.newQueryId(System.nanoTime(), 0),
        1,
        System.currentTimeMillis());

    assertEquals(0, scheduler.getRunningQuery());

    scheduler.submitQuery(schedulingInfo);
    barrier.acquire();
    assertEquals(1, scheduler.getRunningQuery());

    assertEquals(totalResource, scheduler.getMaximumResourceCapability());
    assertEquals(totalResource,
        NodeResources.add(scheduler.getQMMinimumResourceCapability(), scheduler.getClusterResource()));
  }

  @Test(timeout = 10000)
  public void testMaximumSubmitQuery() throws InterruptedException {
    assertEquals(0, scheduler.getRunningQuery());
    int maximumParallelQuery = scheduler.getResourceCalculator().computeAvailableContainers(
        scheduler.getMaximumResourceCapability(), scheduler.getQMMinimumResourceCapability());

    int testParallelNum = 10;
    for (int i = 0; i < testParallelNum; i++) {
      QuerySchedulingInfo schedulingInfo = new QuerySchedulingInfo("default",
          "user",
          QueryIdFactory.newQueryId(System.nanoTime(), 0),
          1,
          System.currentTimeMillis());
      scheduler.submitQuery(schedulingInfo);
    }

    barrier.acquire();
    // allow 50% parallel running
    assertEquals(Math.floor(maximumParallelQuery * 0.5f), (double) scheduler.getRunningQuery(), 1.0f);
    assertEquals(testParallelNum, scheduler.getRunningQuery() + scheduler.getQueryQueue().size());
  }

  @Test(timeout = 10000)
  public void testReserveResource() throws InterruptedException, ExecutionException {
    int requestNum = 3;
    assertEquals(totalResource, scheduler.getMaximumResourceCapability());
    assertEquals(totalResource, scheduler.getClusterResource());

    QueryId queryId = QueryIdFactory.newQueryId(System.nanoTime(), 0);
    CallFuture<NodeResourceResponseProto> callBack = new CallFuture<NodeResourceResponseProto>();
    rmContext.getDispatcher().getEventHandler().handle(new ResourceReserveSchedulerEvent(
        createResourceRequest(queryId, requestNum, new ArrayList<Integer>()), callBack));

    NodeResourceResponseProto responseProto = callBack.get();
    assertEquals(queryId, new QueryId(responseProto.getQueryId()));
    assertEquals(requestNum, responseProto.getResourceCount());

    NodeResource allocations = NodeResources.createResource(0);
    for (AllocationResourceProto resourceProto : responseProto.getResourceList()) {
      NodeResources.addTo(allocations, new NodeResource(resourceProto.getResource()));
    }

    assertEquals(NodeResources.subtract(totalResource, allocations), scheduler.getClusterResource());
  }

  @Test(timeout = 10000)
  public void testReserveResourceWithWorkerPriority() throws InterruptedException, ExecutionException {
    int requestNum = 2;
    assertEquals(totalResource, scheduler.getMaximumResourceCapability());
    assertEquals(totalResource, scheduler.getClusterResource());

    List<Integer> targetWorkers = Lists.newArrayList();
    Map.Entry<Integer, Worker> workerEntry = rmContext.getWorkers().entrySet().iterator().next();
    targetWorkers.add(workerEntry.getKey());

    NodeResource expectResource = NodeResources.multiply(scheduler.getMinimumResourceCapability(), requestNum);
    assertTrue(NodeResources.fitsIn(expectResource, workerEntry.getValue().getAvailableResource()));

    QueryId queryId = QueryIdFactory.newQueryId(System.nanoTime(), 0);
    NodeResourceRequestProto requestProto = createResourceRequest(queryId, requestNum, targetWorkers);
    CallFuture<NodeResourceResponseProto> callBack = new CallFuture<NodeResourceResponseProto>();
    rmContext.getDispatcher().getEventHandler().handle(new ResourceReserveSchedulerEvent(
        requestProto, callBack));

    NodeResourceResponseProto responseProto = callBack.get();
    assertEquals(queryId, new QueryId(responseProto.getQueryId()));
    assertEquals(requestNum, responseProto.getResourceCount());

    for (AllocationResourceProto resourceProto : responseProto.getResourceList()) {
      assertEquals(workerEntry.getKey().intValue(), resourceProto.getWorkerId());
    }
  }

  private NodeResourceRequestProto
  createResourceRequest(QueryId queryId, int containerNum, List<Integer> candidateWorkers) {
    NodeResourceRequestProto.Builder request =
        NodeResourceRequestProto.newBuilder();
    request.setCapacity(scheduler.getMinimumResourceCapability().getProto())
        .setNumContainers(containerNum)
        .setPriority(1)
        .setQueryId(queryId.getProto())
        .setType(ResourceType.LEAF)
        .setUserId("test user")
        .setRunningTasks(0)
        .addAllCandidateNodes(candidateWorkers)
        .setQueue("default");
    return request.build();
  }

  class MySimpleScheduler extends SimpleScheduler {
    Semaphore barrier;
    Map<QueryId, QueryInfo> queryInfoMap = Maps.newHashMap();

    public MySimpleScheduler(TajoRMContext rmContext, Semaphore barrier) {
      super(null, rmContext);
      this.barrier = barrier;
    }

    @Override
    public void submitQuery(QuerySchedulingInfo schedulingInfo) {
      queryInfoMap.put(schedulingInfo.getQueryId(), new QueryInfo(schedulingInfo.getQueryId()) {
        QueryContext context;
        @Override
        public QueryContext getQueryContext() {
          if(context == null) {
            context = new QueryContext(conf);
            context.setUser("user");
          }
          return context;
        }
      });
      super.submitQuery(schedulingInfo);
    }

    @Override
    protected boolean startQuery(QueryId queryId, final AllocationResourceProto allocation) {
      executorService.schedule(new Runnable() {
        @Override
        public void run() {
          barrier.release();
          NodeResources.addTo(rmContext.getWorkers().get(allocation.getWorkerId()).getAvailableResource(),
              new NodeResource(allocation.getResource()));
          rmContext.getDispatcher().getEventHandler().handle(new SchedulerEvent(SchedulerEventType.RESOURCE_UPDATE));
        }
      }, testDelay, TimeUnit.MILLISECONDS);
      return true;
    }

    @Override
    public void handle(SchedulerEvent event) {
      super.handle(event);
      barrier.release();
    }

    @Override
    protected QueryInfo getQueryInfo(QueryId queryId) {
      return queryInfoMap.get(queryId);
    }

    @Override
    public void stopQuery(QueryId queryId) {
      queryInfoMap.remove(queryId);
      super.stopQuery(queryId);
    }
  }
}
