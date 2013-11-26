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

package org.apache.tajo.engine.planner.global;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMasterPlan {

  @Test
  public void testConnect() {
    MasterPlan masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), null, null);

    ExecutionBlock eb1 = masterPlan.newExecutionBlockForTest();
    ExecutionBlock eb2 = masterPlan.newExecutionBlockForTest();
    ExecutionBlock eb3 = masterPlan.newExecutionBlockForTest();

    masterPlan.addConnect(eb1, eb2, TajoWorkerProtocol.PartitionType.LIST_PARTITION);
    assertTrue(masterPlan.isConnected(eb1.getId(), eb2.getId()));
    assertTrue(masterPlan.isReverseConnected(eb2.getId(), eb1.getId()));

    masterPlan.addConnect(eb3, eb2, TajoWorkerProtocol.PartitionType.LIST_PARTITION);
    assertTrue(masterPlan.isConnected(eb1.getId(), eb2.getId()));
    assertTrue(masterPlan.isConnected(eb3.getId(), eb2.getId()));

    assertTrue(masterPlan.isReverseConnected(eb2.getId(), eb1.getId()));
    assertTrue(masterPlan.isReverseConnected(eb2.getId(), eb3.getId()));

    masterPlan.disconnect(eb3, eb2);
    assertFalse(masterPlan.isConnected(eb3, eb2));
    assertFalse(masterPlan.isReverseConnected(eb2, eb3));
  }
}
