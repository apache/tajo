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

package org.apache.tajo.engine.planner.logical;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Target;

public class TableSubQueryNode extends RelationNode implements Projectable {
  @Expose private String tableName;
  @Expose private LogicalNode subQuery;
  @Expose private Target [] targets; // unused

  public TableSubQueryNode(String tableName, LogicalNode subQuery) {
    super(NodeType.TABLE_SUBQUERY);
    this.tableName = tableName;
    this.subQuery = subQuery;
    setOutSchema(PlannerUtil.getQualifiedSchema(this.subQuery.getOutSchema(), tableName));
    setInSchema(this.subQuery.getInSchema());
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public String getCanonicalName() {
    return tableName;
  }

  @Override
  public Schema getTableSchema() {
    return getOutSchema();
  }

  public void setSubQuery(LogicalNode node) {
    this.subQuery = node;
    setInSchema(subQuery.getInSchema());
  }

  public LogicalNode getSubQuery() {
    return subQuery;
  }

  @Override
  public boolean hasTargets() {
    return targets != null;
  }

  @Override
  public void setTargets(Target[] targets) {
    this.targets = targets;
  }

  @Override
  public Target[] getTargets() {
    return targets;
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString("TableSubQuery");
    planStr.appendTitle(" as ").appendTitle(tableName);
    return planStr;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, subQuery);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof TableSubQueryNode) {
      TableSubQueryNode another = (TableSubQueryNode) object;
      return tableName.equals(another.tableName) && subQuery.equals(another.subQuery);
    }

    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    TableSubQueryNode newTableSubQueryNode = (TableSubQueryNode) super.clone();
    newTableSubQueryNode.tableName = tableName;
    return newTableSubQueryNode;
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
    subQuery.preOrder(visitor);
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    subQuery.preOrder(visitor);
    visitor.visit(this);
  }

  public String toString() {
    return "Table Subquery (alias = " + tableName + ")\n" + subQuery.toString();
  }
}
