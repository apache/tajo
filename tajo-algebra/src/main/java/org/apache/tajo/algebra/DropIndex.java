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

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.util.TUtil;

import java.util.Objects;

public class DropIndex extends Expr {

  @Expose @SerializedName("IndexName")
  private String indexName;
  @Expose @SerializedName("IfExists")
  private boolean ifExists;

  public DropIndex(String indexName, boolean ifExists) {
    super(OpType.DropIndex);
    this.indexName = indexName;
    this.ifExists = ifExists;
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexName, ifExists);
  }

  @Override
  boolean equalsTo(Expr expr) {
    if (expr instanceof DropIndex) {
      DropIndex another = (DropIndex) expr;
      return TUtil.checkEquals(indexName, another.indexName) &&
          ifExists == another.ifExists;
    }
    return false;
  }

  public Object clone() throws CloneNotSupportedException {
    DropIndex clone = (DropIndex) super.clone();
    clone.indexName = indexName;
    clone.ifExists = ifExists;
    return clone;
  }

  public String getIndexName() {
    return indexName;
  }

  public boolean isIfExists() {
    return ifExists;
  }
}
