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

public class DropIndexNode extends LogicalNode implements Cloneable {
  private String indexName;

  public DropIndexNode(int pid) {
    super(pid, NodeType.DROP_INDEX);
  }

  public void init(String indexName) {
    this.indexName = indexName;
  }

  public int hashCode() {
    return Objects.hashCode(indexName);
  }

  public boolean equals(Object obj) {
    if (obj instanceof DropIndexNode) {
      DropIndexNode other = (DropIndexNode) obj;
      return super.equals(other) &&
          this.indexName.equals(other.indexName);
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
    return clone;
  }

  @Override
  public String toString() {
    return "DROP INDEX " + indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public String getIndexName() {
    return indexName;
  }
}
