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
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ContainerProtocol;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.*;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.tajo.ipc.TajoResourceTrackerProtocol.NodeHeartbeat;
import static org.junit.Assert.*;

public class TestTajoResourceManager {
  private final PrimitiveProtos.BoolProto BOOL_TRUE = PrimitiveProtos.BoolProto.newBuilder().setValue(true).build();
  private final PrimitiveProtos.BoolProto BOOL_FALSE = PrimitiveProtos.BoolProto.newBuilder().setValue(false).build();

  TajoConf tajoConf;

  long queryIdTime = System.currentTimeMillis();
  int numWorkers = 5;
  float workerDiskSlots = 5.0f;
  int workerMemoryMB = 512 * 10;
  WorkerResourceAllocationResponse response;

  private TajoWorkerResourceManager initResourceManager() throws Exception {
    tajoConf = new org.apache.tajo.conf.TajoConf();

    tajoConf.setFloatVar(TajoConf.ConfVars.TAJO_QUERYMASTER_DISK_SLOT, 0.0f);
    tajoConf.setIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MEMORY_MB, 512);
    tajoConf.setVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS, "localhost:0");
    TajoWorkerResourceManager tajoWorkerResourceManager = new TajoWorkerResourceManager(tajoConf);
    tajoWorkerResourceManager.init(tajoConf);
    tajoWorkerResourceManager.start();

    for(int i = 0; i < numWorkers; i++) {
      ServerStatusProto.System system = ServerStatusProto.System.newBuilder()
          .setAvailableProcessors(1)
          .setFreeMemoryMB(workerMemoryMB)
          .setMaxMemoryMB(workerMemoryMB)
          .setTotalMemoryMB(workerMemoryMB)
          .build();

      ServerStatusProto.JvmHeap jvmHeap = ServerStatusProto.JvmHeap.newBuilder()
          .setFreeHeap(workerMemoryMB)
          .setMaxHeap(workerMemoryMB)
          .setTotalHeap(workerMemoryMB)
          .build();

      ServerStatusProto.Disk disk = ServerStatusProto.Disk.newBuilder()
          .setAbsolutePath("/")
          .setFreeSpace(0)
          .setTotalSpace(0)
          .setUsableSpace(0)
          .build();

      List<ServerStatusProto.Disk> disks = new ArrayList<ServerStatusProto.Disk>();

      disks.add(disk);

      ServerStatusProto serverStatus = ServerStatusProto.newBuilder()
          .setDiskSlots(workerDiskSlots)
          .setMemoryResourceMB(workerMemoryMB)
          .setJvmHeap(jvmHeap)
          .setSystem(system)
          .addAllDisk(disks)
          .setRunningTaskNum(0)
          .build();

      WorkerConnectionInfo connectionInfo =
          new WorkerConnectionInfo("host" + (i + 1), 28091, 28092, 21000 + i, 28093, 28080);
      NodeHeartbeat tajoHeartbeat = NodeHeartbeat.newBuilder()
          .setConnectionInfo(connectionInfo.getProto())
          .setServerStatus(serverStatus)
          .build();

      tajoWorkerResourceManager.getResourceTracker().heartbeat(null, tajoHeartbeat, NullCallback.get());
    }

    return tajoWorkerResourceManager;
  }


  @Test
  public void testHeartbeat() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;
    try {
      tajoWorkerResourceManager = initResourceManager();
      assertEquals(numWorkers, tajoWorkerResourceManager.getWorkers().size());
      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());
        assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
      }
    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }

  @Test
  public void testMemoryResource() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;
    try {
      tajoWorkerResourceManager = initResourceManager();

      final int minMemory = 256;
      final int maxMemory = 512;
      float diskSlots = 1.0f;

      QueryId queryId = QueryIdFactory.newQueryId(queryIdTime, 1);

      WorkerResourceAllocationRequest request = WorkerResourceAllocationRequest.newBuilder()
          .setResourceRequestPriority(ResourceRequestPriority.MEMORY)
          .setNumContainers(60)
          .setQueryId(queryId.getProto())
          .setMaxDiskSlotPerContainer(diskSlots)
          .setMinDiskSlotPerContainer(diskSlots)
          .setMinMemoryMBPerContainer(minMemory)
          .setMaxMemoryMBPerContainer(maxMemory)
          .build();

      final CountDownLatch barrier = new CountDownLatch(1);
      final List<ContainerProtocol.TajoContainerIdProto> containerIds = new
        ArrayList<ContainerProtocol.TajoContainerIdProto>();

      RpcCallback<WorkerResourceAllocationResponse> callBack = new RpcCallback<WorkerResourceAllocationResponse>() {

        @Override
        public void run(WorkerResourceAllocationResponse response) {
          TestTajoResourceManager.this.response = response;
          barrier.countDown();
        }
      };

      tajoWorkerResourceManager.allocateWorkerResources(request, callBack);
      assertTrue(barrier.await(3, TimeUnit.SECONDS));


      // assert after callback
      int totalUsedMemory = 0;
      int totalUsedDisks = 0;

      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(0, resource.getAvailableMemoryMB());
        assertEquals(0, resource.getAvailableDiskSlots(), 0);
        assertEquals(5.0f, resource.getUsedDiskSlots(), 0);

        totalUsedMemory += resource.getUsedMemoryMB();
        totalUsedDisks += resource.getUsedDiskSlots();
      }

      assertEquals(workerMemoryMB * numWorkers, totalUsedMemory);
      assertEquals(workerDiskSlots * numWorkers, totalUsedDisks, 0);

      assertEquals(numWorkers * 10, response.getWorkerAllocatedResourceList().size());

      for(WorkerAllocatedResource eachResource: response.getWorkerAllocatedResourceList()) {
        assertTrue(
            eachResource.getAllocatedMemoryMB() >= minMemory &&  eachResource.getAllocatedMemoryMB() <= maxMemory);
        containerIds.add(eachResource.getContainerId());
      }

      for(ContainerProtocol.TajoContainerIdProto eachContainerId: containerIds) {
        tajoWorkerResourceManager.releaseWorkerResource(eachContainerId);
      }

      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());
        assertEquals(0, resource.getUsedMemoryMB());

        assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
        assertEquals(0.0f, resource.getUsedDiskSlots(), 0);
      }
    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }

  @Test
  public void testMemoryNotCommensurable() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;

    try {
      tajoWorkerResourceManager = initResourceManager();

      final int minMemory = 200;
      final int maxMemory = 500;
      float diskSlots = 1.0f;

      QueryId queryId = QueryIdFactory.newQueryId(queryIdTime, 2);

      int requiredContainers = 60;

      int numAllocatedContainers = 0;

      int loopCount = 0;
      while(true) {
        WorkerResourceAllocationRequest request = WorkerResourceAllocationRequest.newBuilder()
            .setResourceRequestPriority(ResourceRequestPriority.MEMORY)
            .setNumContainers(requiredContainers - numAllocatedContainers)
            .setQueryId(queryId.getProto())
            .setMaxDiskSlotPerContainer(diskSlots)
            .setMinDiskSlotPerContainer(diskSlots)
            .setMinMemoryMBPerContainer(minMemory)
            .setMaxMemoryMBPerContainer(maxMemory)
            .build();

        final CountDownLatch barrier = new CountDownLatch(1);

        RpcCallback<WorkerResourceAllocationResponse> callBack = new RpcCallback<WorkerResourceAllocationResponse>() {
          @Override
          public void run(WorkerResourceAllocationResponse response) {
            TestTajoResourceManager.this.response = response;
            barrier.countDown();
          }
        };

        tajoWorkerResourceManager.allocateWorkerResources(request, callBack);

        assertTrue(barrier.await(3, TimeUnit.SECONDS));

        numAllocatedContainers += TestTajoResourceManager.this.response.getWorkerAllocatedResourceList().size();

        //release resource
        for(WorkerAllocatedResource eachResource:
            TestTajoResourceManager.this.response.getWorkerAllocatedResourceList()) {
          assertTrue(
              eachResource.getAllocatedMemoryMB() >= minMemory &&  eachResource.getAllocatedMemoryMB() <= maxMemory);
          tajoWorkerResourceManager.releaseWorkerResource(eachResource.getContainerId());
        }

        for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
          WorkerResource resource = worker.getResource();
          assertEquals(0, resource.getUsedMemoryMB());
          assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());

          assertEquals(0.0f, resource.getUsedDiskSlots(), 0);
          assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
        }

        loopCount++;

        if(loopCount == 2) {
          assertEquals(requiredContainers, numAllocatedContainers);
          break;
        }
      }

      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(0, resource.getUsedMemoryMB());
        assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());

        assertEquals(0.0f, resource.getUsedDiskSlots(), 0);
        assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
      }
    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }

  @Test
  public void testDiskResource() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;

    try {
      tajoWorkerResourceManager = initResourceManager();

      final float minDiskSlots = 1.0f;
      final float maxDiskSlots = 2.0f;
      int memoryMB = 256;

      QueryId queryId = QueryIdFactory.newQueryId(queryIdTime, 3);

      WorkerResourceAllocationRequest request = WorkerResourceAllocationRequest.newBuilder()
          .setResourceRequestPriority(ResourceRequestPriority.DISK)
          .setNumContainers(60)
          .setQueryId(queryId.getProto())
          .setMaxDiskSlotPerContainer(maxDiskSlots)
          .setMinDiskSlotPerContainer(minDiskSlots)
          .setMinMemoryMBPerContainer(memoryMB)
          .setMaxMemoryMBPerContainer(memoryMB)
          .build();

      final CountDownLatch barrier = new CountDownLatch(1);
      final List<ContainerProtocol.TajoContainerIdProto> containerIds = new
        ArrayList<ContainerProtocol.TajoContainerIdProto>();


      RpcCallback<WorkerResourceAllocationResponse> callBack = new RpcCallback<WorkerResourceAllocationResponse>() {

        @Override
        public void run(WorkerResourceAllocationResponse response) {
          TestTajoResourceManager.this.response = response;
          barrier.countDown();
        }
      };

      tajoWorkerResourceManager.allocateWorkerResources(request, callBack);
      assertTrue(barrier.await(3, TimeUnit.SECONDS));

      for(WorkerAllocatedResource eachResource: response.getWorkerAllocatedResourceList()) {
        assertTrue("AllocatedDiskSlot:" + eachResource.getAllocatedDiskSlots(),
            eachResource.getAllocatedDiskSlots() >= minDiskSlots &&
                eachResource.getAllocatedDiskSlots() <= maxDiskSlots);
        containerIds.add(eachResource.getContainerId());
      }

      // assert after callback
      int totalUsedDisks = 0;
      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        //each worker allocated 3 container (2 disk slot = 2, 1 disk slot = 1)
        assertEquals(0, resource.getAvailableDiskSlots(), 0);
        assertEquals(5.0f, resource.getUsedDiskSlots(), 0);
        assertEquals(256 * 3, resource.getUsedMemoryMB());

        totalUsedDisks += resource.getUsedDiskSlots();
      }

      assertEquals(workerDiskSlots * numWorkers, totalUsedDisks, 0);

      assertEquals(numWorkers * 3, response.getWorkerAllocatedResourceList().size());

      for(ContainerProtocol.TajoContainerIdProto eachContainerId: containerIds) {
        tajoWorkerResourceManager.releaseWorkerResource(eachContainerId);
      }

      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());
        assertEquals(0, resource.getUsedMemoryMB());

        assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
        assertEquals(0.0f, resource.getUsedDiskSlots(), 0);
      }
    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }

  @Test
  public void testDiskResourceWithStoppedQuery() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;

    try {
      tajoWorkerResourceManager = initResourceManager();

      final float minDiskSlots = 1.0f;
      final float maxDiskSlots = 2.0f;
      int memoryMB = 256;

      QueryId queryId = QueryIdFactory.newQueryId(queryIdTime, 3);

      WorkerResourceAllocationRequest request = WorkerResourceAllocationRequest.newBuilder()
          .setResourceRequestPriority(ResourceRequestPriority.DISK)
          .setNumContainers(60)
          .setQueryId(queryId.getProto())
          .setMaxDiskSlotPerContainer(maxDiskSlots)
          .setMinDiskSlotPerContainer(minDiskSlots)
          .setMinMemoryMBPerContainer(memoryMB)
          .setMaxMemoryMBPerContainer(memoryMB)
          .build();

      final CountDownLatch barrier = new CountDownLatch(1);
      final List<ContainerProtocol.TajoContainerIdProto> containerIds = new
        ArrayList<ContainerProtocol.TajoContainerIdProto>();


      RpcCallback<WorkerResourceAllocationResponse> callBack = new RpcCallback<WorkerResourceAllocationResponse>() {

        @Override
        public void run(WorkerResourceAllocationResponse response) {
          TestTajoResourceManager.this.response = response;
          barrier.countDown();
        }
      };

      tajoWorkerResourceManager.getRMContext().getStoppedQueryIds().add(queryId);
      tajoWorkerResourceManager.allocateWorkerResources(request, callBack);
      assertFalse(barrier.await(3, TimeUnit.SECONDS));

      assertNull(response);

      // assert after callback
      int totalUsedDisks = 0;
      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        //each worker allocated 3 container (2 disk slot = 2, 1 disk slot = 1)
        assertEquals(5.0f, resource.getAvailableDiskSlots(), 0);
        assertEquals(0, resource.getUsedDiskSlots(), 0);
        assertEquals(0, resource.getUsedMemoryMB());

        totalUsedDisks += resource.getUsedDiskSlots();
      }

      assertEquals(0, totalUsedDisks, 0);

      for(ContainerProtocol.TajoContainerIdProto eachContainerId: containerIds) {
        tajoWorkerResourceManager.releaseWorkerResource(eachContainerId);
      }

      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());
        assertEquals(0, resource.getUsedMemoryMB());

        assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
        assertEquals(0.0f, resource.getUsedDiskSlots(), 0);
      }
    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }

}
