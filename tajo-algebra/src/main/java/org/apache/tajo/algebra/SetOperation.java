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
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class SetOperation extends BinaryOperator {
  @Expose @SerializedName("IsDistinct")
  private boolean distinct = true;

  public SetOperation(OpType type, Expr left, Expr right, boolean distinct) {
    super(type, left, right);
    Preconditions.checkArgument(type == OpType.Union ||
        type == OpType.Intersect ||
        type == OpType.Except);
    this.distinct = distinct;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public void unsetDistinct() {
    distinct = false;
  }

  public int hashCode() {
    return Objects.hashCode(distinct, getLeft(), getRight());
  }

  boolean equalsTo(Expr expr) {
    SetOperation another = (SetOperation) expr;
    return distinct == another.distinct;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SetOperation setOperation = (SetOperation) super.clone();
    setOperation.distinct = distinct;
    return setOperation;
  }
}
