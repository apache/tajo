/**
 * 
 */
package nta.engine.query;

import nta.engine.QueryIdFactory;
import nta.engine.ipc.QueryUnitReport;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author jihoon
 *
 */
public class TestQueryUnitReportImpl {

  @Test
  public void test() {
    QueryIdFactory.reset();
    QueryUnitReport r1 = new QueryUnitReportImpl(QueryIdFactory.newQueryUnitId(), 10.f);
    QueryUnitReport r2 = new QueryUnitReportImpl(r1.getProto());
    
    assertEquals(r1.getId(), r2.getId());
    assertEquals(r1.getProgress(), r2.getProgress(), 1e-8);
  }
}
