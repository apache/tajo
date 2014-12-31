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

package org.apache.tajo.plan.logical;

import com.google.common.base.Objects;
import org.apache.tajo.plan.PlanString;

public class DropTableNode extends LogicalNode implements Cloneable {
  private String tableName;
  private boolean ifExists;
  private boolean purge;

  public DropTableNode(int pid) {
    super(pid, NodeType.DROP_TABLE);
  }

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public LogicalNode getChild(int idx) {
    return null;
  }

  public void init(String tableName, boolean ifExists, boolean purge) {
    this.tableName = tableName;
    this.ifExists = ifExists;
    this.purge = purge;
  }

  public String getTableName() {
    return this.tableName;
  }

  public boolean isIfExists() {
    return this.ifExists;
  }

  public boolean isPurge() {
    return this.purge;
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this).appendTitle(ifExists ? " IF EXISTS" : "").appendTitle(purge ? " PURGE" : "");
  }

  public int hashCode() {
    return Objects.hashCode(tableName, ifExists, purge);
  }

  public boolean equals(Object obj) {
    if (obj instanceof DropTableNode) {
      DropTableNode other = (DropTableNode) obj;
      return super.equals(other) &&
          this.tableName.equals(other.tableName) &&
          this.ifExists == other.ifExists &&
          this.purge == other.purge;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DropTableNode dropTableNode = (DropTableNode) super.clone();
    dropTableNode.tableName = tableName;
    dropTableNode.ifExists = ifExists;
    dropTableNode.purge = purge;
    return dropTableNode;
  }

  @Override
  public String toString() {
    return "DROP TABLE " + (ifExists ? "IF EXISTS " : "") + tableName + (purge ? " PURGE" : "");
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
