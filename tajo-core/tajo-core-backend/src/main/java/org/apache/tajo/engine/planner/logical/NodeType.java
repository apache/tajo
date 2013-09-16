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

/**
 *
 */
package org.apache.tajo.engine.planner.logical;

import org.apache.tajo.engine.planner.InsertNode;

/**
 * This indicates a logical node type.
 */
public enum NodeType {
  BST_INDEX_SCAN(IndexScanNode.class),
  CREATE_TABLE(CreateTableNode.class),
  DROP_TABLE(DropTableNode.class),
  EXCEPT(ExceptNode.class),
  EXPRS(EvalExprNode.class),
  GROUP_BY(GroupbyNode.class),
  INSERT(InsertNode.class),
  INTERSECT(IntersectNode.class),
  LIMIT(LimitNode.class),
  JOIN(JoinNode.class),
  PROJECTION(ProjectionNode.class),
  RECEIVE(ReceiveNode.class),
  ROOT(LogicalRootNode.class),
  SEND(SendNode.class),
  SCAN(ScanNode.class),
  SELECTION(SelectionNode.class),
  STORE(StoreTableNode.class),
  SORT(SortNode.class),
  UNION(UnionNode.class),
  TABLE_SUBQUERY(TableSubQueryNode.class);


  private final Class<? extends LogicalNode> baseClass;

  NodeType(Class<? extends LogicalNode> baseClass) {
    this.baseClass = baseClass;
  }

  public Class<? extends LogicalNode> getBaseClass() {
    return this.baseClass;
  }
}
