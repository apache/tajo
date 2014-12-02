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

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.IndexMeta;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.util.KeyValueSet;

import java.net.URI;

import static org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;

public class CreateIndexNode extends UnaryNode implements Cloneable {
  @Expose private IndexMeta indexMeta;

  public CreateIndexNode(int pid) {
    super(pid, NodeType.CREATE_INDEX);
    this.indexMeta = new IndexMeta();
  }

  public void setUnique(boolean unique) {
    indexMeta.setUnique(unique);
  }

  public boolean isUnique() {
    return indexMeta.isUnique();
  }

  public void setIndexName(String indexName) {
    indexMeta.setIndexName(indexName);
  }

  public String getIndexName() {
    return indexMeta.getIndexName();
  }

  public void setIndexPath(URI indexPath) {
    indexMeta.setIndexPath(indexPath);
  }

  public URI getIndexPath() {
    return indexMeta.getIndexPath();
  }

  public void setKeySortSpecs(Schema targetRelationSchema, SortSpec[] sortSpecs) {
    indexMeta.setKeySortSpecs(targetRelationSchema, sortSpecs);
  }

  public SortSpec[] getKeySortSpecs() {
    return indexMeta.getKeySortSpecs();
  }

  public void setIndexMethod(IndexMethod indexType) {
    indexMeta.setIndexMethod(indexType);
  }

  public IndexMethod getIndexMethod() {
    return indexMeta.getIndexMethod();
  }

  public void setOptions(KeyValueSet options) {
    indexMeta.setOptions(options);
  }

  public KeyValueSet getOptions() {
    return indexMeta.getOptions();
  }

  public Schema getTargetRelationSchema() {
    return indexMeta.getTargetRelationSchema();
  }

  @Override
  public int hashCode() {
    return indexMeta.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CreateIndexNode) {
      CreateIndexNode other = (CreateIndexNode) obj;
      return this.indexMeta.equals(other.indexMeta);
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    CreateIndexNode createIndexNode = (CreateIndexNode) super.clone();
    createIndexNode.indexMeta = (IndexMeta) this.indexMeta.clone();
    return createIndexNode;
  }

  private String getSortSpecString() {
    StringBuilder sb = new StringBuilder("Column [key= ");
    SortSpec[] sortSpecs = indexMeta.getKeySortSpecs();
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
    return "CreateIndex (indexName=" + indexMeta.getIndexName() + ", indexPath=" + indexMeta.getIndexPath() +
        ", type=" + indexMeta.getIndexMethod().name() +
        ", isUnique=" + indexMeta.isUnique() + ", " + getSortSpecString() + ")";
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this);
  }
}
