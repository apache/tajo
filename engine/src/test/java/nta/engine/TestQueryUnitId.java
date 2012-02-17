package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

/**
 * @author Hyunsik Choi
 */
public class TestQueryUnitId {
  @Test
  public void testQueryIds() {
    Date dateNow = new Date();
    SimpleDateFormat dateformatYYYYMMDD = new SimpleDateFormat("yyyyMMddSS");
    String timeId = dateformatYYYYMMDD.format(dateNow);
    
    QueryId queryId = new QueryId(timeId, 1);
    assertEquals("query_" + timeId + "_001", queryId.toString());
    
    SubQueryId subId = new SubQueryId(queryId, 2);
    assertEquals("query_" + timeId+"_001_002", subId.toString());
    
    QueryStepId stepId = new QueryStepId(subId, 4);
    assertEquals("query_" + timeId+"_001_002_004", stepId.toString());
    
    QueryUnitId qId = new QueryUnitId(stepId, 5);
    assertEquals("query_" + timeId + "_001_002_004_000005", qId.toString());
  }

  @Test
  public void testEqualsObject() {
    Date dateNow = new Date();
    SimpleDateFormat dateformatYYYYMMDD = new SimpleDateFormat("yyyyMMddSS");
    String timeId = dateformatYYYYMMDD.format(dateNow);
    
    QueryId queryId1 = new QueryId(timeId, 1);
    QueryId queryId2 = new QueryId(timeId, 2);    
    assertNotSame(queryId1, queryId2);    
    QueryId queryId3 = new QueryId(timeId, 1);
    assertEquals(queryId1, queryId3);
    
    SubQueryId sid1 = new SubQueryId(queryId1, 1);
    SubQueryId sid2 = new SubQueryId(queryId1, 2);    
    assertNotSame(sid1, sid2);
    SubQueryId sid3 = new SubQueryId(queryId1, 1);
    assertEquals(sid1, sid3);
    
    QueryStepId stepId1 = new QueryStepId(sid1, 9);
    QueryStepId stepId2 = new QueryStepId(sid1, 10);
    assertNotSame(stepId1, stepId2);
    QueryStepId stepId3 = new QueryStepId(sid1, 9);
    assertEquals(stepId1, stepId3);
    
    QueryUnitId qid1 = new QueryUnitId(stepId1, 9);
    QueryUnitId qid2 = new QueryUnitId(stepId1, 10);
    assertNotSame(qid1, qid2);
    QueryUnitId qid3 = new QueryUnitId(stepId1, 9);
    assertEquals(qid1, qid3);
  }

  @Test
  public void testCompareTo() {
    Date dateNow = new Date();
    SimpleDateFormat dateformatYYYYMMDD = new SimpleDateFormat("yyyyMMddSS");
    String timeId = dateformatYYYYMMDD.format(dateNow);
    
    QueryId queryId1 = new QueryId(timeId, 1);
    QueryId queryId2 = new QueryId(timeId, 2);
    QueryId queryId3 = new QueryId(timeId, 1);
    assertEquals(-1, queryId1.compareTo(queryId2));
    assertEquals(1, queryId2.compareTo(queryId1));
    assertEquals(0, queryId3.compareTo(queryId1));
    
    SubQueryId sid1 = new SubQueryId(queryId1, 1);
    SubQueryId sid2 = new SubQueryId(queryId1, 2);    
    SubQueryId sid3 = new SubQueryId(queryId1, 1);
    assertEquals(-1, sid1.compareTo(sid2));
    assertEquals(1, sid2.compareTo(sid1));
    assertEquals(0, sid3.compareTo(sid1));
    
    QueryStepId step1 = new QueryStepId(sid1, 9);
    QueryStepId step2 = new QueryStepId(sid1, 10);
    QueryStepId step3 = new QueryStepId(sid1, 9);
    assertEquals(-1, step1.compareTo(step2));
    assertEquals(1, step2.compareTo(step1));
    assertEquals(0, step3.compareTo(step1));
    
    QueryUnitId qid1 = new QueryUnitId(step1, 9);
    QueryUnitId qid2 = new QueryUnitId(step1, 10);
    QueryUnitId qid3 = new QueryUnitId(step1, 9);
    assertEquals(-1, qid1.compareTo(qid2));
    assertEquals(1, qid2.compareTo(qid1));
    assertEquals(0, qid3.compareTo(qid1));
  }
}
