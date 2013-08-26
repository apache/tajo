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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestQueryIdFactory {
  
  @Before
  public void setup() {
  }

  @Test
  public void testNewQueryId() {
    QueryId qid1 = QueryIdFactory.newQueryId();
    QueryId qid2 = QueryIdFactory.newQueryId();
    assertTrue(qid1.compareTo(qid2) < 0);
  }
  
  @Test
  public void testNewSubQueryId() {
    QueryId qid = QueryIdFactory.newQueryId();
    ExecutionBlockId subqid1 = QueryIdFactory.newExecutionBlockId(qid);
    ExecutionBlockId subqid2 = QueryIdFactory.newExecutionBlockId(qid);
    assertTrue(subqid1.compareTo(subqid2) < 0);
  }
  
  @Test
  public void testNewQueryUnitId() {
    QueryId qid = QueryIdFactory.newQueryId();
    ExecutionBlockId subid = QueryIdFactory.newExecutionBlockId(qid);
    QueryUnitId quid1 = QueryIdFactory.newQueryUnitId(subid);
    QueryUnitId quid2 = QueryIdFactory.newQueryUnitId(subid);
    assertTrue(quid1.compareTo(quid2) < 0);
  }
}
