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


import org.apache.tajo.engine.planner.AlterTablespaceNode;

/**
 * This indicates a logical node type.
 */
public enum NodeType {
  ROOT(LogicalRootNode.class),
  EXPRS(EvalExprNode.class),
  PROJECTION(ProjectionNode.class),
  LIMIT(LimitNode.class),
  SORT(SortNode.class),
  HAVING(HavingNode.class),
  GROUP_BY(GroupbyNode.class),
  WINDOW_AGG(WindowAggNode.class),
  SELECTION(SelectionNode.class),
  JOIN(JoinNode.class),
  UNION(UnionNode.class),
  EXCEPT(ExceptNode.class),
  INTERSECT(IntersectNode.class),
  TABLE_SUBQUERY(TableSubQueryNode.class),
  SCAN(ScanNode.class),
  PARTITIONS_SCAN(PartitionedTableScanNode.class),
  BST_INDEX_SCAN(IndexScanNode.class),
  STORE(StoreTableNode.class),
  INSERT(InsertNode.class),
  DISTINCT_GROUP_BY(DistinctGroupbyNode.class),

  CREATE_DATABASE(CreateDatabaseNode.class),
  DROP_DATABASE(DropDatabaseNode.class),
  CREATE_TABLE(CreateTableNode.class),
  DROP_TABLE(DropTableNode.class),
  ALTER_TABLESPACE (AlterTablespaceNode.class),
  ALTER_TABLE (AlterTableNode.class),
  TRUNCATE_TABLE (TruncateTableNode.class);

  private final Class<? extends LogicalNode> baseClass;

  NodeType(Class<? extends LogicalNode> baseClass) {
    this.baseClass = baseClass;
  }

  public Class<? extends LogicalNode> getBaseClass() {
    return this.baseClass;
  }
}
