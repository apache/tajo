package org.apache.tajo.plan.joinorder;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.logical.LogicalNode;

import java.util.Set;

public interface JoinVertex {

  Schema getSchema();
  LogicalNode getCorrespondingNode();
  Set<RelationVertex> getRelations();
}
