/*
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

package tajo;

import org.junit.Test;
import tajo.util.TajoIdUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author Hyunsik Choi
 */
public class TestQueryUnitId {
  @Test
  public void testQueryId() {
    long ts1 = 1315890136000l;
    long ts2 = 1315890136001l;
    QueryId j1 = TajoIdUtils.createQueryId(ts1, 2);
    QueryId j2 = TajoIdUtils.createQueryId(ts1, 1);
    QueryId j3 = TajoIdUtils.createQueryId(ts2, 1);
    QueryId j4 = TajoIdUtils.createQueryId(ts1, 2);

    assertTrue(j1.equals(j4));
    assertFalse(j1.equals(j2));
    assertFalse(j1.equals(j3));

    assertTrue(j1.compareTo(j4) == 0);
    assertTrue(j1.compareTo(j2) > 0);
    assertTrue(j1.compareTo(j3) < 0);

    assertTrue(j1.hashCode() == j4.hashCode());
    assertFalse(j1.hashCode() == j2.hashCode());
    assertFalse(j1.hashCode() == j3.hashCode());

    QueryId j5 = TajoIdUtils.createQueryId(ts1, 231415);
    assertEquals("query_" + ts1 + "_0002", j1.toString());
    assertEquals("query_" + ts1 + "_231415", j5.toString());
  }

  @Test
  public void testQueryIds() {
    long timeId = 1315890136000l;
    
    QueryId queryId = TajoIdUtils.createQueryId(timeId, 1);
    assertEquals("query_" + timeId + "_0001", queryId.toString());
    
    SubQueryId subId = TajoIdUtils.newSubQueryId(queryId, 2);
    assertEquals("query_" + timeId +"_0001_02", subId.toString());
    
    ScheduleUnitId logicalQUeryUnitId =
        new ScheduleUnitId(subId, 6);
    assertEquals("query_" + timeId +"_0001_02_006",
        logicalQUeryUnitId.toString());
    
    QueryUnitId qId = new QueryUnitId(logicalQUeryUnitId, 5);
    assertEquals("query_" + timeId + "_0001_02_006_000005", qId.toString());
  }

  @Test
  public void testEqualsObject() {
    long timeId = System.currentTimeMillis();
    
    QueryId queryId1 = TajoIdUtils.createQueryId(timeId, 1);
    QueryId queryId2 = TajoIdUtils.createQueryId(timeId, 2);
    assertNotSame(queryId1, queryId2);    
    QueryId queryId3 = TajoIdUtils.createQueryId(timeId, 1);
    assertEquals(queryId1, queryId3);
    
    SubQueryId sid1 = TajoIdUtils.newSubQueryId(queryId1, 1);
    SubQueryId sid2 = TajoIdUtils.newSubQueryId(queryId1, 2);    
    assertNotSame(sid1, sid2);
    SubQueryId sid3 = TajoIdUtils.newSubQueryId(queryId1, 1);
    assertEquals(sid1, sid3);
    
    ScheduleUnitId lqid1 = new ScheduleUnitId(sid1, 9);
    ScheduleUnitId lqid2 = new ScheduleUnitId(sid1, 10);
    assertNotSame(lqid1, lqid2);
    ScheduleUnitId lqid3 = new ScheduleUnitId(sid1, 9);
    assertEquals(lqid1, lqid3);
    
    QueryUnitId qid1 = new QueryUnitId(lqid1, 9);
    QueryUnitId qid2 = new QueryUnitId(lqid1, 10);
    assertNotSame(qid1, qid2);
    QueryUnitId qid3 = new QueryUnitId(lqid1, 9);
    assertEquals(qid1, qid3);
  }

  @Test
  public void testCompareTo() {
    long time = System.currentTimeMillis();
    
    QueryId queryId1 = TajoIdUtils.createQueryId(time, 1);
    QueryId queryId2 = TajoIdUtils.createQueryId(time, 2);
    QueryId queryId3 = TajoIdUtils.createQueryId(time, 1);
    assertEquals(-1, queryId1.compareTo(queryId2));
    assertEquals(1, queryId2.compareTo(queryId1));
    assertEquals(0, queryId3.compareTo(queryId1));
    
    SubQueryId sid1 = TajoIdUtils.newSubQueryId(queryId1, 1);
    SubQueryId sid2 = TajoIdUtils.newSubQueryId(queryId1, 2);    
    SubQueryId sid3 = TajoIdUtils.newSubQueryId(queryId1, 1);
    assertEquals(-1, sid1.compareTo(sid2));
    assertEquals(1, sid2.compareTo(sid1));
    assertEquals(0, sid3.compareTo(sid1));
    
    ScheduleUnitId lqid1 = new ScheduleUnitId(sid1, 9);
    ScheduleUnitId lqid2 = new ScheduleUnitId(sid1, 10);
    ScheduleUnitId lqid3 = new ScheduleUnitId(sid1, 9);
    assertEquals(-1, lqid1.compareTo(lqid2));
    assertEquals(1, lqid2.compareTo(lqid1));
    assertEquals(0, lqid3.compareTo(lqid1));
    
    QueryUnitId qid1 = new QueryUnitId(lqid1, 9);
    QueryUnitId qid2 = new QueryUnitId(lqid1, 10);
    QueryUnitId qid3 = new QueryUnitId(lqid1, 9);
    assertEquals(-1, qid1.compareTo(qid2));
    assertEquals(1, qid2.compareTo(qid1));
    assertEquals(0, qid3.compareTo(qid1));
  }
  
  @Test
  public void testConstructFromString() {
    QueryIdFactory.reset();
    QueryId qid1 = QueryIdFactory.newQueryId();
    QueryId qid2 = TajoIdUtils.createQueryId(qid1.toString());
    assertEquals(qid1, qid2);
    
    SubQueryId sub1 = QueryIdFactory.newSubQueryId(qid1);
    SubQueryId sub2 = TajoIdUtils.newSubQueryId(sub1.toString());
    assertEquals(sub1, sub2);
    
    ScheduleUnitId lqid1 = QueryIdFactory.newScheduleUnitId(sub1);
    ScheduleUnitId lqid2 = new ScheduleUnitId(lqid1.toString());
    assertEquals(lqid1, lqid2);
    
    QueryUnitId u1 = QueryIdFactory.newQueryUnitId(lqid1);
    QueryUnitId u2 = new QueryUnitId(u1.toString());
    assertEquals(u1, u2);
  }
}
