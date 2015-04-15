/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.joinorder;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.RelationNode;
import org.apache.tajo.util.TUtil;

import java.util.Set;

public class RelationVertex implements JoinVertex{

  private final RelationNode relationNode;
  private final LogicalNode topLogicalNode;

public RelationVertex(RelationNode relationNode) {
    this.relationNode = relationNode;
    this.topLogicalNode = relationNode;
  }

  @Override
  public String toString() {
    return relationNode.getCanonicalName();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RelationVertex) {
      RelationVertex other = (RelationVertex) o;
      return this.relationNode.equals(other.relationNode) && this.topLogicalNode.equals(other.topLogicalNode);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return relationNode.hashCode();
  }

  @Override
  public Schema getSchema() {
    return relationNode.getOutSchema();
  }

  @Override
  public Set<RelationVertex> getRelations() {
    return TUtil.newHashSet(this);
  }

  @Override
  public LogicalNode buildPlan(LogicalPlan plan, LogicalPlan.QueryBlock block) {
    return topLogicalNode;
  }

  public LogicalNode getRelationNode() {
    return relationNode;
  }
}
