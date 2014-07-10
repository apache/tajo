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

public class Join extends BinaryOperator {
  @Expose @SerializedName("JoinType")
  private JoinType joinType;
  @Expose @SerializedName("JoinCondition")
  private Expr joinQual;
  @Expose @SerializedName("JoinColumns")
  private ColumnReferenceExpr[] joinColumns;
  @Expose @SerializedName("IsNatural")
  private boolean natural = false;

  public Join(JoinType joinType) {
    super(OpType.Join);
    this.joinType = joinType;
  }

  public JoinType getJoinType() {
    return  this.joinType;
  }

  public boolean hasQual() {
    return this.joinQual != null;
  }

  public Expr getQual() {
    return this.joinQual;
  }

  public void setQual(Expr expr) {
    this.joinQual = expr;
  }

  public boolean hasJoinColumns() {
    return joinColumns != null;
  }

  public ColumnReferenceExpr[] getJoinColumns() {
    return joinColumns;
  }

  public void setJoinColumns(ColumnReferenceExpr[] columns) {
    joinColumns = columns;
  }

  public void setNatural() {
    natural = true;
  }

  public boolean isNatural() {
    return natural;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(joinType, joinQual, Objects.hashCode(joinColumns), natural);
  }

  boolean equalsTo(Expr expr) {
    Join another = (Join) expr;
    return joinType.equals(another.joinType) &&
        TUtil.checkEquals(joinQual, another.joinQual) &&
        TUtil.checkEquals(joinColumns, another.joinColumns) &&
        natural == another.natural;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Join join = (Join) super.clone();
    join.joinType = joinType;
    join.joinQual = (Expr) joinQual.clone();
    if (joinColumns != null) {
      join.joinColumns = new ColumnReferenceExpr[joinColumns.length];
      for (ColumnReferenceExpr colume : joinColumns) {
        join.joinColumns = (ColumnReferenceExpr[]) colume.clone();
      }
    }
    join.natural = natural;
    return join;
  }
}
