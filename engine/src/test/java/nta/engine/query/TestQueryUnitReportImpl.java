/**
 * 
 */
package nta.engine.query;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import nta.engine.QueryIdFactory;
import nta.engine.LeafServerProtos.QueryStatus;
import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.ipc.QueryUnitReport;

import org.junit.Test;

/**
 * @author jihoon
 * @author Hyunsik Choi
 */
public class TestQueryUnitReportImpl {

  @Test
  public void test() {
    QueryIdFactory.reset();
    
    List<InProgressStatus> list 
      = new ArrayList<InProgressStatus>();
    
    InProgressStatus.Builder builder = InProgressStatus.newBuilder()
        .setId(QueryIdFactory.newQueryUnitId().toString())
        .setProgress(0.5f)
        .setStatus(QueryStatus.FINISHED);
    list.add(builder.build());
    
    builder = InProgressStatus.newBuilder()
        .setId(QueryIdFactory.newQueryUnitId().toString())
        .setProgress(0.5f)
        .setStatus(QueryStatus.FINISHED);
    list.add(builder.build());
    
    QueryUnitReport r1 = new QueryUnitReportImpl(list);
    QueryUnitReport r2 = new QueryUnitReportImpl(r1.getProto());
    
    assertEquals(r1.getProto().getStatusCount(), r2.getProto().getStatusCount());
    assertEquals(r1.getProto(), r2.getProto());
    assertEquals(r1.getProgressList().size(), r2.getProgressList().size());
  }
}
