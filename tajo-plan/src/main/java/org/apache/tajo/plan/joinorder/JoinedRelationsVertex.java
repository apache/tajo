package org.apache.tajo.plan.joinorder;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;

public class JoinedRelationsVertex implements JoinVertex {

  private final JoinEdge joinEdge;
  private final Schema schema;
  private final JoinNode joinNode; // corresponding join node

  public JoinedRelationsVertex(JoinEdge joinEdge, JoinNode joinNode) {
    this.joinEdge = joinEdge;
    this.schema = SchemaUtil.merge(joinEdge.getLeftVertex().getSchema(),
        joinEdge.getRightVertex().getSchema());
    this.joinNode = joinNode;
  }

  public JoinEdge getJoinEdge() {
    return this.joinEdge;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return joinEdge.toString();
  }

  public LogicalNode getCorrespondingNode() {
    return joinNode;
  }

  public JoinType getJoinType() {
    return joinEdge.getJoinType();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof JoinedRelationsVertex) {
      return this.joinEdge.equals(((JoinedRelationsVertex) o).joinEdge);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return joinEdge.hashCode();
  }

}
