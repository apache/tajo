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

package org.apache.tajo;

import org.apache.tajo.engine.planner.global.MasterPlan;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestQueryIdFactory {
  
  @Before
  public void setup() {
  }

  @Test
  public void testNewQueryId() {
    QueryId qid1 = LocalTajoTestingUtility.newQueryId();
    QueryId qid2 = LocalTajoTestingUtility.newQueryId();
    assertTrue(qid1.compareTo(qid2) < 0);
  }
  
  @Test
  public void testNewSubQueryId() {
    QueryId qid = LocalTajoTestingUtility.newQueryId();
    MasterPlan plan = new MasterPlan(qid, null, null);
    ExecutionBlockId subqid1 = plan.newExecutionBlockId();
    ExecutionBlockId subqid2 = plan.newExecutionBlockId();
    assertTrue(subqid1.compareTo(subqid2) < 0);
  }
  
  @Test
  public void testNewTaskId() {
    QueryId qid = LocalTajoTestingUtility.newQueryId();
    MasterPlan plan = new MasterPlan(qid, null, null);
    ExecutionBlockId subid = plan.newExecutionBlockId();
    TaskId quid1 = QueryIdFactory.newTaskId(subid);
    TaskId quid2 = QueryIdFactory.newTaskId(subid);
    assertTrue(quid1.compareTo(quid2) < 0);
  }
}
