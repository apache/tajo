package tajo.engine.planner.logical.extended;

import org.junit.Test;
import tajo.engine.planner.logical.ExprType;

import java.net.URI;

import static org.junit.Assert.assertEquals;

/**
 * @author Hyunsik Choi
 */
public class TestSendNode {
  @Test
  public final void testSendNode() throws CloneNotSupportedException {
    SendNode send = new SendNode(PipeType.PULL, RepartitionType.HASH);
    send.putDestURI(0, URI.create("http://localhost:2190"));
    send.putDestURI(1, URI.create("http://localhost:2191"));
    
    assertEquals(ExprType.SEND, send.getType());
    assertEquals(PipeType.PULL, send.getPipeType());
    assertEquals(RepartitionType.HASH, send.getRepartitionType());
    assertEquals(URI.create("http://localhost:2190"), send.getDestURI(0));
    assertEquals(URI.create("http://localhost:2191"), send.getDestURI(1));
    
    SendNode send2 = (SendNode) send.clone();
    assertEquals(ExprType.SEND, send2.getType());
    assertEquals(PipeType.PULL, send2.getPipeType());
    assertEquals(RepartitionType.HASH, send2.getRepartitionType());
    assertEquals(URI.create("http://localhost:2190"), send2.getDestURI(0));
    assertEquals(URI.create("http://localhost:2191"), send2.getDestURI(1));
    
    assertEquals(send, send2);
  }
}
