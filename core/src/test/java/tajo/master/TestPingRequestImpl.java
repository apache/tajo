/**
 * 
 */
package tajo.master;

import org.junit.Test;
import tajo.engine.MasterInterfaceProtos.InProgressStatusProto;
import tajo.engine.MasterInterfaceProtos.QueryStatus;
import tajo.QueryIdFactory;
import tajo.engine.ipc.PingRequest;
import tajo.engine.query.PingRequestImpl;
import tajo.engine.utils.TUtil;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
        .setId(TUtil.newQueryUnitAttemptId().getProto())
        .setProgress(0.5f)
        .setStatus(QueryStatus.QUERY_FINISHED);
    list.add(builder.build());
    
    builder = InProgressStatusProto.newBuilder()
        .setId(TUtil.newQueryUnitAttemptId().getProto())
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
