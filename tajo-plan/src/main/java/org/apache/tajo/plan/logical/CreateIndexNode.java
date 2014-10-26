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
import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;

public class CreateIndexNode extends UnaryNode implements Cloneable {
  @Expose private boolean isUnique;
  @Expose private String indexName;
  @Expose private Path indexPath;
  @Expose private SortSpec[] sortSpecs;
  @Expose private IndexMethod indexType = IndexMethod.TWO_LEVEL_BIN_TREE;
  @Expose private KeyValueSet options;

  public CreateIndexNode(int pid) {
    super(pid, NodeType.CREATE_INDEX);
  }

  public void setUnique(boolean unique) {
    this.isUnique = unique;
  }

  public boolean isUnique() {
    return isUnique;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public String getIndexName() {
    return this.indexName;
  }

  public void setIndexPath(Path indexPath) {
    this.indexPath = indexPath;
  }

  public Path getIndexPath() {
    return this.indexPath;
  }

  public void setSortSpecs(SortSpec[] sortSpecs) {
    this.sortSpecs = sortSpecs;
  }

  public SortSpec[] getSortSpecs() {
    return this.sortSpecs;
  }

  public void setIndexType(IndexMethod indexType) {
    this.indexType = indexType;
  }

  public IndexMethod getIndexType() {
    return this.indexType;
  }

  public void setOptions(KeyValueSet options) {
    this.options = options;
  }

  public KeyValueSet getOptions() {
    return this.options;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(isUnique, indexName, indexPath, sortSpecs, indexType, options);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CreateIndexNode) {
      CreateIndexNode other = (CreateIndexNode) obj;
      return this.isUnique == other.isUnique &&
          TUtil.checkEquals(this.indexName, other.indexName) &&
          TUtil.checkEquals(this.indexPath, other.indexPath) &&
          TUtil.checkEquals(this.sortSpecs, other.sortSpecs) &&
          this.indexType.equals(other.indexType) &&
          TUtil.checkEquals(this.options, other.options);
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    CreateIndexNode createIndexNode = (CreateIndexNode) super.clone();
    createIndexNode.isUnique = isUnique;
    createIndexNode.indexName = indexName;
    createIndexNode.indexPath = indexPath;
    createIndexNode.sortSpecs = sortSpecs.clone();
    createIndexNode.indexType = indexType;
    createIndexNode.options = (KeyValueSet) (options != null ? options.clone() : null);
    return createIndexNode;
  }

  private String getSortSpecString() {
    StringBuilder sb = new StringBuilder("Column [key= ");
    for (int i = 0; i < sortSpecs.length; i++) {
      sb.append(sortSpecs[i].getSortKey().getQualifiedName()).append(" ")
          .append(sortSpecs[i].isAscending() ? "asc" : "desc");
      if(i < sortSpecs.length - 1) {
        sb.append(",");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public String toString() {
    return "CreateIndex (indexName=" + indexName + ", indexPath=" + indexPath + ", type=" + indexType.name() +
        ", isUnique=" + isUnique + ", " + getSortSpecString() + ")";
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this);
  }
}
