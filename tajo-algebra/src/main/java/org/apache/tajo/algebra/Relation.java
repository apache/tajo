/**
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
import org.apache.tajo.util.TUtil;

public class Relation extends Expr {
  @Expose @SerializedName("TableName")
  private String tableName;
  @Expose @SerializedName("TableAlias")
  private String alias;

  protected Relation(OpType type, String relationName) {
    super(type);
    this.tableName = relationName;
  }

  public Relation(String relationName) {
    this(OpType.Relation, relationName);
  }

  public String getCanonicalName() {
    return alias == null ? tableName : alias;
  }

  public String getName() {
    return tableName;
  }

  public boolean hasAlias() {
    return alias != null;
  }

  public String getAlias() {
    return this.alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, alias);
  }

  @Override
  boolean equalsTo(Expr expr) {
    Relation other = (Relation) expr;
    return TUtil.checkEquals(tableName, other.tableName) &&
        TUtil.checkEquals(alias, other.alias);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Relation relation = (Relation) super.clone();
    relation.tableName = tableName;
    relation.alias = alias;
    return relation;
  }
}
