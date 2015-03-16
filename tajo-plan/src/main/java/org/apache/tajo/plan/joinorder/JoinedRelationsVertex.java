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
import org.apache.tajo.plan.logical.LogicalNode;
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

  public LogicalNode getCorrespondingNode() {
    return joinEdge.getCorrespondingJoinNode();
  }

  @Override
  public Set<RelationVertex> getRelations() {
    return relations;
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
