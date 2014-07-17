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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CaseWhenPredicate extends Expr {
  @Expose @SerializedName("WhenExprs")
  private List<WhenExpr> whens = new ArrayList<WhenExpr>();
  @Expose @SerializedName("ElseExpr")
  private Expr elseResult;

  public CaseWhenPredicate() {
    super(OpType.CaseWhen);
  }

  public void addWhen(Expr condition, Expr result) {
    whens.add(new WhenExpr(condition, result));
  }

  public Collection<WhenExpr> getWhens() {
    return this.whens;
  }

  public void setElseResult(Expr elseResult) {
    this.elseResult = elseResult;
  }

  public Expr getElseResult() {
    return this.elseResult;
  }

  public boolean hasElseResult() {
    return elseResult != null;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(whens, elseResult);
  }

  @Override
  boolean equalsTo(Expr expr) {
    CaseWhenPredicate another = (CaseWhenPredicate) expr;
    return whens.equals(another.whens) && TUtil.checkEquals(elseResult, another.elseResult);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    CaseWhenPredicate caseWhen = (CaseWhenPredicate) super.clone();
    caseWhen.whens = new ArrayList<WhenExpr>();
    for (int i = 0; i < whens.size(); i++) {
      caseWhen.whens.add((WhenExpr) whens.get(i).clone());
    }
    caseWhen.elseResult = elseResult != null ? (Expr) elseResult.clone() : null;
    return caseWhen;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  public static class WhenExpr implements Cloneable {
    @Expose @SerializedName("Condition")
    Expr condition;
    @Expose @SerializedName("Result")
    Expr result;

    public WhenExpr(Expr condition, Expr result) {
      this.condition = condition;
      this.result = result;
    }

    public void setCondition(Expr condition) {
      this.condition = condition;
    }

    public Expr getCondition() {
      return this.condition;
    }

    public void setResult(Expr result) {
      this.result = result;
    }

    public Expr getResult() {
      return this.result;
    }

    public int hashCode() {
      return Objects.hashCode(condition, result);
    }

    public boolean equals(Object obj) {
      if (obj instanceof WhenExpr) {
        WhenExpr another = (WhenExpr) obj;
        return TUtil.checkEquals(condition, another.condition) && TUtil.checkEquals(result, another.result);
      }

      return false;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      WhenExpr when = (WhenExpr) super.clone();
      when.condition = (Expr) condition.clone();
      when.result = (Expr) result.clone();
      return when;
    }
  }
}
