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

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.util.TUtil;

public class DropIndexNode extends LogicalNode implements Cloneable {
  @Expose private String indexName;
  @Expose private boolean ifExists;

  public DropIndexNode(int pid) {
    super(pid, NodeType.DROP_INDEX);
  }

  public void init(String indexName, boolean ifExists) {
    this.indexName = indexName;
    this.ifExists = ifExists;
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
    return new PlanString(this).appendTitle(ifExists ? " IF EXISTS" : "");
  }

  @Override
  public String toString() {
    return "DROP INDEX " + (ifExists ? "IF EXISTS " : "") + indexName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(indexName, ifExists);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DropIndexNode) {
      DropIndexNode other = (DropIndexNode) o;
      return TUtil.checkEquals(this.indexName, other.indexName) &&
          this.ifExists == other.ifExists;
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DropIndexNode clone = (DropIndexNode) super.clone();
    clone.indexName = this.indexName;
    clone.ifExists = this.ifExists;
    return clone;
  }

  public String getIndexName() {
    return indexName;
  }

  public boolean isIfExists() {
    return ifExists;
  }
}
