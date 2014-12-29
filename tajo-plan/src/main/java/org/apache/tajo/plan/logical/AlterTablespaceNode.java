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

package org.apache.tajo.plan.logical;


import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.algebra.AlterTablespaceSetType;
import org.apache.tajo.plan.PlanString;

public class AlterTablespaceNode extends LogicalNode implements Cloneable {

  @Expose private String tablespaceName;
  @Expose private AlterTablespaceSetType setType;
  @Expose private String uri;


  public AlterTablespaceNode(int pid) {
    super(pid, NodeType.ALTER_TABLESPACE);
  }

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public LogicalNode getChild(int idx) {
    return null;
  }

  public String getTablespaceName() {
    return tablespaceName;
  }

  public void setTablespaceName(String tablespaceName) {
    this.tablespaceName = tablespaceName;
  }

  public AlterTablespaceSetType getSetType() {
    return setType;
  }

  public String getLocation() {
    return uri;
  }

  public void setLocation(String uri) {
    this.setType = AlterTablespaceSetType.LOCATION;
    this.uri = uri;
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AlterTablespaceNode) {
      AlterTablespaceNode other = (AlterTablespaceNode) obj;
      return super.equals(other);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(tablespaceName, setType, uri);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    AlterTablespaceNode alter = (AlterTablespaceNode) super.clone();
    alter.tablespaceName = tablespaceName;
    alter.setType = setType;
    alter.uri = uri;
    return alter;
  }

  @Override
  public String toString() {
    return "AlterTablespace (space=" + tablespaceName + ")";
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
}
