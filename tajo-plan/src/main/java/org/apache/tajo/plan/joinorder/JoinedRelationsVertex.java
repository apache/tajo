/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.joinorder;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.RelationNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.TUtil;

import java.util.Set;

/**
 * The join order is fixed in this vertex.
 */
public class JoinedRelationsVertex implements JoinVertex {

  private final Set<RelationVertex> relations = TUtil.newHashSet();
  private final JoinEdge joinEdge;

  public JoinedRelationsVertex(JoinEdge joinEdge) {
    this.joinEdge = joinEdge;
    findRelationVertexes(this.joinEdge);
  }

  private void findRelationVertexes(JoinEdge edge) {
    if (edge.getLeftVertex() instanceof JoinedRelationsVertex) {
      JoinedRelationsVertex leftChild = (JoinedRelationsVertex) edge.getLeftVertex();
      findRelationVertexes(leftChild.getJoinEdge());
    } else {
      relations.add((RelationVertex) edge.getLeftVertex());
    }
    if (edge.getRightVertex() instanceof JoinedRelationsVertex) {
      JoinedRelationsVertex rightChild = (JoinedRelationsVertex) edge.getRightVertex();
      findRelationVertexes(rightChild.getJoinEdge());
    } else {
      relations.add((RelationVertex) edge.getRightVertex());
    }
  }

  public JoinEdge getJoinEdge() {
    return joinEdge;
  }

  @Override
  public Schema getSchema() {
    return joinEdge.getSchema();
  }

  @Override
  public String toString() {
    return joinEdge.toString();
  }

  @Override
  public Set<RelationVertex> getRelations() {
    return relations;
  }

  @Override
  public LogicalNode buildPlan(LogicalPlan plan, LogicalPlan.QueryBlock block) {
    // TODO
    LogicalNode leftChild = joinEdge.getLeftVertex().buildPlan(plan, block);
    LogicalNode rightChild = joinEdge.getRightVertex().buildPlan(plan, block);

    JoinNode joinNode = plan.createNode(JoinNode.class);

    if (PlannerUtil.isSymmetricJoin(joinEdge.getJoinType())) {
      // if only one operator is relation
      if ((leftChild instanceof RelationNode) && !(rightChild instanceof RelationNode)) {
        // for left deep
        joinNode.init(joinEdge.getJoinType(), rightChild, leftChild);
      } else {
        // if both operators are relation or if both are relations
        // we don't need to concern the left-right position.
        joinNode.init(joinEdge.getJoinType(), leftChild, rightChild);
      }
    } else {
      joinNode.init(joinEdge.getJoinType(), leftChild, rightChild);
    }

    Schema mergedSchema = SchemaUtil.merge(joinNode.getLeftChild().getOutSchema(),
        joinNode.getRightChild().getOutSchema());
    joinNode.setInSchema(mergedSchema);
    joinNode.setOutSchema(mergedSchema);
    if (joinEdge.hasJoinQual()) {
      joinNode.setJoinQual(joinEdge.getSingletonJoinQual());
    }
    block.registerNode(joinNode);
    return joinNode;
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
