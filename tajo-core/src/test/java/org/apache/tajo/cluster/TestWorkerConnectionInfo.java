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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestWorkerConnectionInfo {

  @Test
  public void testWorkerId() {
    WorkerConnectionInfo worker = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
    WorkerConnectionInfo worker2 = new WorkerConnectionInfo("host2", 28091, 28092, 21000, 28093, 28080);

    assertNotEquals(worker.getId(), worker2.getId());
    assertNotEquals(worker.getId(), new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080).getId());
  }
  
  @Test
  public void testWorkerIdUniqueness() {
    ConcurrentMap<Integer, WorkerConnectionInfo> connectionInfoMap =
        new ConcurrentHashMap<Integer, WorkerConnectionInfo>();
    WorkerConnectionInfo worker = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
    connectionInfoMap.put(worker.getId(), worker);
    
    for (int i = 0; i < 10000; i++) {
      worker = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
      assertNull(connectionInfoMap.putIfAbsent(worker.getId(), worker));
    }
  }
}
