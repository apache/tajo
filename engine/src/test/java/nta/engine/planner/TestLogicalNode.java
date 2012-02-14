package nta.engine.planner;

import static org.junit.Assert.assertEquals;
import nta.engine.planner.logical.LogicalNode;

/**
 * @author Hyunsik Choi
 */
public class TestLogicalNode {  
  public static final void testCloneLogicalNode(LogicalNode n1) 
      throws CloneNotSupportedException {
    LogicalNode copy = (LogicalNode) n1.clone();
    assertEquals(n1, copy);
  }
}
