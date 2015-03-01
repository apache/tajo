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

package org.apache.tajo.algebra;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.algebra.Sort.SortSpec;
import org.apache.tajo.util.TUtil;

import java.util.Map;

public class CreateIndex extends UnaryOperator {
  @Expose @SerializedName("IsUnique")
  private boolean unique = false;
  @Expose @SerializedName("IndexName")
  private String indexName;
  @Expose @SerializedName("SortSpecs")
  private SortSpec[] sortSpecs;
  @Expose @SerializedName("Properties")
  private Map<String, String> params;
  @Expose @SerializedName("IndexMethodSpec")
  private IndexMethodSpec methodSpec;
  private boolean external = false;
  @Expose @SerializedName("IndexPath")
  private String indexPath;

  public CreateIndex(final String indexName, final SortSpec[] sortSpecs) {
    super(OpType.CreateIndex);
    this.indexName = indexName;
    this.sortSpecs = sortSpecs;
    this.methodSpec = new IndexMethodSpec("TWO_LEVEL_BIN_TREE");
  }

  public void setUnique(boolean unique) {
    this.unique = unique;
  }

  public boolean isUnique() {
    return this.unique;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setSortSpecs(SortSpec[] sortSpecs) {
    this.sortSpecs = sortSpecs;
  }

  public SortSpec[] getSortSpecs() {
    return sortSpecs;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }

  public Map<String, String> getParams() {
    return this.params;
  }

  public void setMethodSpec(IndexMethodSpec methodSpec) {
    this.methodSpec = methodSpec;
  }

  public IndexMethodSpec getMethodSpec() {
    return this.methodSpec;
  }

  public void setIndexPath(String indexPath) {
    this.external = true;
    this.indexPath = indexPath;
  }

  public boolean isExternal() {
    return this.external;
  }

  public String getIndexPath() {
    return this.indexPath;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(unique, indexName,  sortSpecs, params, methodSpec, external);
  }

  @Override
  boolean equalsTo(Expr expr) {
    CreateIndex other = (CreateIndex) expr;
    return this.unique == other.unique &&
        this.indexName.equals(other.indexName) &&
        TUtil.checkEquals(this.sortSpecs, other.sortSpecs) &&
        TUtil.checkEquals(this.params, other.params) &&
        this.methodSpec.equals(other.methodSpec) &&
        this.external == other.external;
  }

  public static class IndexMethodSpec {
    @Expose @SerializedName("IndexMethodName")
    private String name;

    public IndexMethodSpec(final String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof IndexMethodSpec) {
        IndexMethodSpec other = (IndexMethodSpec) o;
        return this.name.equals(other.name);
      }
      return false;
    }
  }
}
