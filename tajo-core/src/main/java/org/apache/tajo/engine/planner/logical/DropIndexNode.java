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

package org.apache.tajo.engine.planner.logical;

import org.apache.tajo.engine.planner.PlanString;

import java.util.Objects;

public class DropIndexNode extends LogicalNode implements Cloneable {
  private String databaseName;
  private String indexName;

  public DropIndexNode(int pid) {
    super(pid, NodeType.DROP_INDEX);
  }

  public void init(String databaseName, String indexName) {
    this.databaseName = databaseName;
    this.indexName = indexName;
  }

  public int hashCode() {
    return Objects.hash(databaseName, indexName);
  }

  public boolean equals(Object obj) {
    if (obj instanceof DropIndexNode) {
      DropIndexNode other = (DropIndexNode) obj;
      return super.equals(other) &&
          this.indexName.equals(other.indexName) &&
          this.databaseName.equals(other.databaseName);
    }
    return false;
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DropIndexNode clone = (DropIndexNode) super.clone();
    clone.indexName = this.indexName;
    clone.databaseName = this.databaseName;
    return clone;
  }

  @Override
  public String toString() {
    return "DROP INDEX " + databaseName + "." + indexName;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getDatabaseName() {
    return databaseName;
  }
}
