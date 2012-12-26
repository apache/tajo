package tajo.engine.planner;

import tajo.engine.planner.logical.LogicalNode;

import static org.junit.Assert.assertEquals;

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
