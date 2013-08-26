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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.tajo.util.TajoIdUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestTajoIds {
  @Test
  public void testQueryId() {
    long ts1 = 1315890136000l;
    long ts2 = 1315890136001l;

    QueryId j1 = createQueryId(ts1, 2);
    QueryId j2 = createQueryId(ts1, 1);
    QueryId j3 = createQueryId(ts2, 1);
    QueryId j4 = createQueryId(ts1, 2);

    assertTrue(j1.equals(j4));
    assertFalse(j1.equals(j2));
    assertFalse(j1.equals(j3));

    assertTrue(j1.compareTo(j4) == 0);
    assertTrue(j1.compareTo(j2) > 0);
    assertTrue(j1.compareTo(j3) < 0);

    assertTrue(j1.hashCode() == j4.hashCode());
    assertFalse(j1.hashCode() == j2.hashCode());
    assertFalse(j1.hashCode() == j3.hashCode());

    QueryId j5 = createQueryId(ts1, 231415);
    assertEquals("q_" + ts1 + "_0002", j1.toString());
    assertEquals("q_" + ts1 + "_231415", j5.toString());
  }

  @Test
  public void testQueryIds() {
    long timeId = 1315890136000l;
    
    QueryId queryId = createQueryId(timeId, 1);
    assertEquals("q_" + timeId + "_0001", queryId.toString());
    
    ExecutionBlockId subId = QueryIdFactory.newExecutionBlockId(queryId, 2);
    assertEquals("eb_" + timeId +"_0001_000002", subId.toString());
    
    QueryUnitId qId = new QueryUnitId(subId, 5);
    assertEquals("t_" + timeId + "_0001_000002_000005", qId.toString());

    QueryUnitAttemptId attemptId = new QueryUnitAttemptId(qId, 4);
    assertEquals("ta_" + timeId + "_0001_000002_000005_04", attemptId.toString());
  }

  @Test
  public void testEqualsObject() {
    long timeId = System.currentTimeMillis();
    
    QueryId queryId1 = createQueryId(timeId, 1);
    QueryId queryId2 = createQueryId(timeId, 2);
    assertNotSame(queryId1, queryId2);    
    QueryId queryId3 = createQueryId(timeId, 1);
    assertEquals(queryId1, queryId3);
    
    ExecutionBlockId sid1 = QueryIdFactory.newExecutionBlockId(queryId1, 1);
    ExecutionBlockId sid2 = QueryIdFactory.newExecutionBlockId(queryId1, 2);
    assertNotSame(sid1, sid2);
    ExecutionBlockId sid3 = QueryIdFactory.newExecutionBlockId(queryId1, 1);
    assertEquals(sid1, sid3);
    
    QueryUnitId qid1 = new QueryUnitId(sid1, 9);
    QueryUnitId qid2 = new QueryUnitId(sid1, 10);
    assertNotSame(qid1, qid2);
    QueryUnitId qid3 = new QueryUnitId(sid1, 9);
    assertEquals(qid1, qid3);
  }

  @Test
  public void testCompareTo() {
    long time = System.currentTimeMillis();
    
    QueryId queryId1 = createQueryId(time, 1);
    QueryId queryId2 = createQueryId(time, 2);
    QueryId queryId3 = createQueryId(time, 1);
    assertEquals(-1, queryId1.compareTo(queryId2));
    assertEquals(1, queryId2.compareTo(queryId1));
    assertEquals(0, queryId3.compareTo(queryId1));

    ExecutionBlockId sid1 = QueryIdFactory.newExecutionBlockId(queryId1, 1);
    ExecutionBlockId sid2 = QueryIdFactory.newExecutionBlockId(queryId1, 2);
    ExecutionBlockId sid3 = QueryIdFactory.newExecutionBlockId(queryId1, 1);
    assertEquals(-1, sid1.compareTo(sid2));
    assertEquals(1, sid2.compareTo(sid1));
    assertEquals(0, sid3.compareTo(sid1));
    
    QueryUnitId qid1 = new QueryUnitId(sid1, 9);
    QueryUnitId qid2 = new QueryUnitId(sid1, 10);
    QueryUnitId qid3 = new QueryUnitId(sid1, 9);
    assertEquals(-1, qid1.compareTo(qid2));
    assertEquals(1, qid2.compareTo(qid1));
    assertEquals(0, qid3.compareTo(qid1));
  }
  
  @Test
  public void testConstructFromString() {
//    QueryIdFactory.reset();
    QueryId qid1 = QueryIdFactory.newQueryId();
    QueryId qid2 = TajoIdUtils.parseQueryId(qid1.toString());
    assertEquals(qid1, qid2);

    ExecutionBlockId sub1 = QueryIdFactory.newExecutionBlockId(qid1);
    ExecutionBlockId sub2 = TajoIdUtils.createExecutionBlockId(sub1.toString());
    assertEquals(sub1, sub2);
    
    QueryUnitId u1 = QueryIdFactory.newQueryUnitId(sub1);
    QueryUnitId u2 = new QueryUnitId(u1.getProto());
    assertEquals(u1, u2);

    QueryUnitAttemptId attempt1 = new QueryUnitAttemptId(u1, 1);
    QueryUnitAttemptId attempt2 = new QueryUnitAttemptId(attempt1.getProto());
    assertEquals(attempt1, attempt2);
  }

  @Test
  public void testConstructFromPB() {
//    QueryIdFactory.reset();
    QueryId qid1 = QueryIdFactory.newQueryId();
    QueryId qid2 = new QueryId(qid1.getProto());
    assertEquals(qid1, qid2);

    ExecutionBlockId sub1 = QueryIdFactory.newExecutionBlockId(qid1);
    ExecutionBlockId sub2 = TajoIdUtils.createExecutionBlockId(sub1.toString());
    assertEquals(sub1, sub2);

    QueryUnitId u1 = QueryIdFactory.newQueryUnitId(sub1);
    QueryUnitId u2 = new QueryUnitId(u1.getProto());
    assertEquals(u1, u2);

    QueryUnitAttemptId attempt1 = new QueryUnitAttemptId(u1, 1);
    QueryUnitAttemptId attempt2 = new QueryUnitAttemptId(attempt1.getProto());
    assertEquals(attempt1, attempt2);
  }

  public static QueryId createQueryId(long timestamp, int id) {
    ApplicationId appId = BuilderUtils.newApplicationId(timestamp, id);

    return QueryIdFactory.newQueryId(appId.toString());
  }
}
