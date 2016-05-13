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
import org.apache.tajo.schema.IdentifierUtil;

public class CreateDatabaseNode extends LogicalNode implements Cloneable {
  private String databaseName;
  private boolean ifNotExists;

  public CreateDatabaseNode(int pid) {
    super(pid, NodeType.CREATE_DATABASE);
  }

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public LogicalNode getChild(int idx) {
    return null;
  }

  public void init(String databaseName, boolean ifNotExists) {
    this.databaseName = databaseName;
    this.ifNotExists = ifNotExists;
  }

  public String getDatabaseName() {
    return this.databaseName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this).appendTitle(ifNotExists ? " IF NOT EXISTS " : " ").appendTitle(databaseName);
  }

  public int hashCode() {
    return Objects.hashCode(databaseName, ifNotExists);
  }

  public boolean equals(Object obj) {
    if (obj instanceof CreateDatabaseNode) {
      CreateDatabaseNode other = (CreateDatabaseNode) obj;
      return super.equals(other) && this.databaseName.equals(other.databaseName) && ifNotExists == other.ifNotExists;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    CreateDatabaseNode newNode = (CreateDatabaseNode) super.clone();
    newNode.databaseName = databaseName;
    newNode.ifNotExists = ifNotExists;
    return newNode;
  }

  @Override
  public String toString() {
    return "CREATE DATABASE " + (ifNotExists ? " IF NOT EXISTS " : "")
        + IdentifierUtil.denormalizeIdentifier(databaseName);
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
