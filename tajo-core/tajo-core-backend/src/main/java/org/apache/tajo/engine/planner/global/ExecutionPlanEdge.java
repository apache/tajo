/*
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

package org.apache.tajo.engine.planner.global;

import org.apache.tajo.engine.planner.logical.LogicalNode;

public class ExecutionPlanEdge {
  public static enum EdgeType {
    LEFT,
    RIGHT,
    SINGLE
  }

  private final Integer parentId;
  private final Integer childId;
  private final EdgeType edgeType;

  public ExecutionPlanEdge(LogicalNode child, LogicalNode parent, EdgeType edgeType) {
    this.parentId = parent.getPID();
    this.childId = child.getPID();
    this.edgeType = edgeType;
  }

  public Integer getParentId() {
    return parentId;
  }

  public Integer getChildId() {
    return childId;
  }

  public EdgeType getEdgeType() {
    return edgeType;
  }
}
