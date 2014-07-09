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

public class SignedExpr extends UnaryOperator {
  @Expose @SerializedName("IsNegative")
  private boolean negative;

  public SignedExpr(boolean negative, Expr operand) {
    super(OpType.Sign);
    this.negative = negative;
    setChild(operand);
  }

  public boolean isNegative() {
    return negative;
  }

  public int hashCode() {
    return Objects.hashCode(negative, getChild());
  }

  @Override
  boolean equalsTo(Expr expr) {
    return negative == ((SignedExpr)expr).negative;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SignedExpr signedExpr = (SignedExpr) super.clone();
    signedExpr.negative = negative;
    return signedExpr;
  }
}
