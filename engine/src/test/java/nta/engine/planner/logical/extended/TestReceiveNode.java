package nta.engine.planner.logical.extended;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.List;

import nta.engine.planner.logical.ExprType;

import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author Hyunsik Choi
 */
public class TestReceiveNode {
  @Test
  public final void testReceiveNode() throws CloneNotSupportedException {
    ReceiveNode rec = new ReceiveNode(PipeType.PULL, RepartitionType.HASH);
    
    URI uri1 = URI.create("http://192.168.0.1:2190/?part=0");
    URI uri2 = URI.create("http://192.168.0.2:2190/?part=1");
    URI uri3 = URI.create("http://192.168.0.3:2190/?part=2");
    URI uri4 = URI.create("http://192.168.0.4:2190/?part=3");
    List<URI> set1 = Lists.newArrayList(uri1, uri2);
    List<URI> set2 = Lists.newArrayList(uri3, uri4);
    
    rec.addData("test1", set1.get(0));
    rec.addData("test1", set1.get(1));
    rec.addData("test2", set2.get(0));
    rec.addData("test2", set2.get(1));
    
    assertEquals(ExprType.RECEIVE, rec.getType());
    assertEquals(PipeType.PULL, rec.getPipeType());
    assertEquals(RepartitionType.HASH, rec.getRepartitionType());    
    assertEquals(set1, Lists.newArrayList(rec.getSrcURIs("test1")));
    assertEquals(set2, Lists.newArrayList(rec.getSrcURIs("test2")));
    
    ReceiveNode rec2 = (ReceiveNode) rec.clone();
    assertEquals(ExprType.RECEIVE, rec2.getType());
    assertEquals(PipeType.PULL, rec2.getPipeType());
    assertEquals(RepartitionType.HASH, rec2.getRepartitionType());    
    assertEquals(set1, Lists.newArrayList(rec2.getSrcURIs("test1")));
    assertEquals(set2, Lists.newArrayList(rec2.getSrcURIs("test2")));
    
    assertEquals(rec, rec2);
  }
}
