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

import org.apache.tajo.util.TUtil;

public class Relation extends Expr {
  private String rel_name;
  private String alias;

  protected Relation(ExprType type, String relationName) {
    super(type);
    this.rel_name = relationName;
  }

  public Relation(String relationName) {
    this(ExprType.Relation, relationName);
  }

  public String getName() {
    return rel_name;
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
  boolean equalsTo(Expr expr) {
    Relation other = (Relation) expr;
    return TUtil.checkEquals(rel_name, other.rel_name) &&
        TUtil.checkEquals(alias, other.alias);
  }

  @Override
  public int hashCode() {
    int result = rel_name.hashCode();
    result = 31 * result + (alias != null ? alias.hashCode() : 0);
    return result;
  }
}
