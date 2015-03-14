package org.apache.tajo.plan.joinorder;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.logical.LogicalNode;

public interface JoinVertex {

  Schema getSchema();
  LogicalNode getCorrespondingNode();
}
