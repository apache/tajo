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

package org.apache.tajo.cluster;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.rm.Worker;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TajoWorker;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TestWorkerConnectionInfo extends QueryTestCaseBase {

  public TestWorkerConnectionInfo() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public void testWorkerId() {
    WorkerConnectionInfo worker = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
    WorkerConnectionInfo worker2 = new WorkerConnectionInfo("host2", 28091, 28092, 21000, 28093, 28080);

    assertEquals(WorkerConnectionInfo.UNALLOCATED_WORKER_ID, worker.getId());
    assertEquals(worker.getId(), worker2.getId());
    assertEquals(worker.getId(), new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080).getId());
  }

  private final List<TajoWorker> additionalWorkerList = TUtil.newList();

  private void createSeveralWorkers() throws Exception {
    for (int workerIndex = 0; workerIndex < 10; workerIndex++) {
      TajoWorker tajoWorker = new TajoWorker();

      TajoConf workerConf  = new TajoConf(testingCluster.getConfiguration());

      workerConf.setVar(TajoConf.ConfVars.WORKER_INFO_ADDRESS, "localhost:0");
      workerConf.setVar(TajoConf.ConfVars.WORKER_CLIENT_RPC_ADDRESS, "localhost:0");
      workerConf.setVar(TajoConf.ConfVars.WORKER_PEER_RPC_ADDRESS, "localhost:0");

      workerConf.setVar(TajoConf.ConfVars.WORKER_QM_RPC_ADDRESS, "localhost:0");

      tajoWorker.startWorker(workerConf, new String[0]);

      additionalWorkerList.add(tajoWorker);
    }
  }

  @Test(timeout = 60000)
  public void testIsWorkerIdGenarated() throws Exception {
    createSeveralWorkers();
    Set<Integer> workerIdSet = TUtil.newHashSet();

    Thread.sleep(30 * 1000);

    assertTrue(testingCluster.getTajoWorkers().size() > 0);

    for (TajoWorker tajoWorker: testingCluster.getTajoWorkers()) {
      int workerId = tajoWorker.getWorkerContext().getConnectionInfo().getId();
      assertNotEquals(WorkerConnectionInfo.UNALLOCATED_WORKER_ID, workerId);

      assertTrue(workerIdSet.add(workerId));
    }

    assertTrue(additionalWorkerList.size() > 0);

    for (TajoWorker tajoWorker: additionalWorkerList) {
      int workerId = tajoWorker.getWorkerContext().getConnectionInfo().getId();
      assertNotEquals(WorkerConnectionInfo.UNALLOCATED_WORKER_ID, workerId);

      assertTrue(workerIdSet.add(workerId));
    }
  }
}
