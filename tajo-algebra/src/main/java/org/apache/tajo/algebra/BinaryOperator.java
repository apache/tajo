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

public class BinaryOperator extends Expr {
  @Expose @SerializedName("LeftExpr")
  protected Expr left;
  @Expose @SerializedName("RightExpr")
  protected Expr right;

  BinaryOperator(OpType opType) {
    super(opType);
  }

  public BinaryOperator(OpType type, Expr left, Expr right) {
    super(type);
    this.left = left;
    this.right = right;
  }

  public <T extends Expr> T getLeft() {
    return (T) this.left;
  }

  public void setLeft(Expr left) {
    this.left = left;
  }

  public <T extends Expr> T getRight() {
    return (T) this.right;
  }

  public void setRight(Expr right) {
    this.right = right;
  }

  public int hashCode() {
    return Objects.hashCode(opType, left, right);
  }

  @Override
  boolean equalsTo(Expr expr) {
    // Operator type is compared at Expr.equals(Object). So, we don't need to compare it here.
    return true;
  }

  public String toString() {
    return left.toString() + " " + opType.toString() + " " + right.toString();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    BinaryOperator binaryOperator = (BinaryOperator) super.clone();
    binaryOperator.left = (Expr) left.clone();
    binaryOperator.right = (Expr) right.clone();
    return binaryOperator;
  }
}
