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

public class DropIndex extends Expr {
  @Expose @SerializedName("IndexName")
  private String indexName;

  public DropIndex(final String indexName) {
    super(OpType.DropIndex);
    this.indexName = indexName;
  }

  @Override
  public int hashCode() {
    return indexName.hashCode();
  }

  @Override
  boolean equalsTo(Expr expr) {
    DropIndex other = (DropIndex) expr;
    return this.indexName.equals(other.indexName);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DropIndex clone = (DropIndex) super.clone();
    clone.indexName = indexName;
    return clone;
  }

  public String getIndexName() {
    return indexName;
  }
}
