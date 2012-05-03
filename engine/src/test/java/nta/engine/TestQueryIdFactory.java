/**
 * 
 */
package nta.engine;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author jihoon
 *
 */
public class TestQueryIdFactory {
  
  @Before
  public void setup() {
    QueryIdFactory.reset();
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
    SubQueryId subqid1 = QueryIdFactory.newSubQueryId(qid);
    SubQueryId subqid2 = QueryIdFactory.newSubQueryId(qid);
    assertTrue(subqid1.compareTo(subqid2) < 0);
  }
  
  @Test
  public void testNewQueryUnitId() {
    QueryId qid = QueryIdFactory.newQueryId();
    SubQueryId subid = QueryIdFactory.newSubQueryId(qid);
    ScheduleUnitId schid = QueryIdFactory.newScheduleUnitId(subid);
    QueryUnitId quid1 = QueryIdFactory.newQueryUnitId(schid);
    QueryUnitId quid2 = QueryIdFactory.newQueryUnitId(schid);
    assertTrue(quid1.compareTo(quid2) < 0);
  }
}
