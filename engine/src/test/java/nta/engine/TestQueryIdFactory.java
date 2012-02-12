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
    SubQueryId qid1 = QueryIdFactory.newSubQueryId();
    SubQueryId qid2 = QueryIdFactory.newSubQueryId();
    assertTrue(qid1.compareTo(qid2) < 0);
  }
  
  @Test
  public void testNewQueryUnitId() {
    QueryUnitId qid1 = QueryIdFactory.newQueryUnitId();
    QueryUnitId qid2 = QueryIdFactory.newQueryUnitId();
    assertTrue(qid1.compareTo(qid2) < 0);
  }
}
