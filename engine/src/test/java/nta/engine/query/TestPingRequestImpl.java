/**
 * 
 */
package nta.engine.query;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.QueryIdFactory;
import nta.engine.ipc.PingRequest;

import org.junit.Test;

/**
 * @author jihoon
 * @author Hyunsik Choi
 */
public class TestPingRequestImpl {

  @Test
  public void test() {
    QueryIdFactory.reset();
    
    List<InProgressStatusProto> list 
      = new ArrayList<InProgressStatusProto>();
    
    InProgressStatusProto.Builder builder = InProgressStatusProto.newBuilder()
        .setId(QueryIdFactory.newQueryUnitId(
            QueryIdFactory.newScheduleUnitId(
                QueryIdFactory.newSubQueryId(
                    QueryIdFactory.newQueryId()))).getProto())
        .setProgress(0.5f)
        .setStatus(QueryStatus.QUERY_FINISHED);
    list.add(builder.build());
    
    builder = InProgressStatusProto.newBuilder()
        .setId(QueryIdFactory.newQueryUnitId(
            QueryIdFactory.newScheduleUnitId(
                QueryIdFactory.newSubQueryId(
                    QueryIdFactory.newQueryId()))).getProto())
        .setProgress(0.5f)
        .setStatus(QueryStatus.QUERY_FINISHED);
    list.add(builder.build());
    
    PingRequest r1 = new PingRequestImpl(System.currentTimeMillis(), 
        "testserver", list);
    PingRequest r2 = new PingRequestImpl(r1.getProto());
    
    assertEquals(r1.getProto().getStatusCount(), r2.getProto().getStatusCount());
    assertEquals(r1.getProto(), r2.getProto());
    assertEquals(r1.getProgressList().size(), r2.getProgressList().size());
  }
}
