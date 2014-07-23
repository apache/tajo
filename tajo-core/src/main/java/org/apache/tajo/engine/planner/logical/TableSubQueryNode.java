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
import org.apache.tajo.engine.utils.SchemaUtil;

public class TableSubQueryNode extends RelationNode implements Projectable {
  @Expose private String tableName;
  @Expose private LogicalNode subQuery;
  @Expose private Target [] targets; // unused

  public TableSubQueryNode(int pid) {
    super(pid, NodeType.TABLE_SUBQUERY);
  }

  public void init(String tableName, LogicalNode subQuery) {
    this.tableName = tableName;
    if (subQuery != null) {
      this.subQuery = subQuery;
      setOutSchema(SchemaUtil.clone(this.subQuery.getOutSchema()));
      setInSchema(SchemaUtil.clone(this.subQuery.getOutSchema()));
      getInSchema().setQualifier(this.tableName);
      getOutSchema().setQualifier(this.tableName);
    }
  }

  @Override
  public boolean hasAlias() {
    return false;
  }

  @Override
  public String getAlias() {
    return null;
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
    // an output schema can be determined by targets. So, an input schema of
    // TableSubQueryNode is only eligible for table schema.
    //
    // TODO - but, a derived table can have column alias. For that, we should improve here.
    //
    // example) select * from (select col1, col2, col3 from t1) view (c1, c2);

    return getInSchema();
  }

  public void setSubQuery(LogicalNode node) {
    this.subQuery = node;
    setInSchema(SchemaUtil.clone(this.subQuery.getOutSchema()));
    getInSchema().setQualifier(this.tableName);
    if (hasTargets()) {
      setOutSchema(PlannerUtil.targetToSchema(targets));
    } else {
      setOutSchema(SchemaUtil.clone(this.subQuery.getOutSchema()));
    }
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
    setOutSchema(PlannerUtil.targetToSchema(targets));
  }

  @Override
  public Target[] getTargets() {
    return targets;
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this);
    planStr.appendTitle(" as ").appendTitle(tableName);

    if (hasTargets()) {
      StringBuilder sb = new StringBuilder("Targets: ");
      for (int i = 0; i < targets.length; i++) {
        sb.append(targets[i]);
        if( i < targets.length - 1) {
          sb.append(", ");
        }
      }
      planStr.addExplan(sb.toString());
      if (getOutSchema() != null) {
        planStr.addExplan("out schema: " + getOutSchema().toString());
      }
      if (getInSchema() != null) {
        planStr.addExplan("in  schema: " + getInSchema().toString());
      }
    }

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
    newTableSubQueryNode.subQuery = (LogicalNode) subQuery.clone();
    if (hasTargets()) {
      newTableSubQueryNode.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        newTableSubQueryNode.targets[i] = (Target) targets[i].clone();
      }
    }
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
    return "Inline view (name=" + tableName + ")";
  }
}
