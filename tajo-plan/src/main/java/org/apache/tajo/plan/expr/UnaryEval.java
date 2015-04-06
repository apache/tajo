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

package org.apache.tajo.plan.expr;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;

public abstract class UnaryEval extends EvalNode implements Cloneable {
  @Expose protected EvalNode child;

  public UnaryEval(EvalType type) {
    super(type);
  }

  public UnaryEval(EvalType type, EvalNode child) {
    super(type);
    this.child = child;
  }

  @Override
  public int childNum() {
    return 1;
  }

  public EvalNode getChild(int idx) {
    Preconditions.checkArgument(idx == 0, "UnaryEval always has one child.");
    return child;
  }

  public void setChild(EvalNode child) {
    this.child = child;
  }

  public EvalNode getChild() {
    return child;
  }

  @Override
  public TajoDataTypes.DataType getValueType() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public <T extends Datum> T eval(Tuple tuple) {
    super.eval(tuple);
    return null;
  }

  @Override
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
    child.preOrder(visitor);
  }

  @Override
  public void postOrder(EvalNodeVisitor visitor) {
    child.postOrder(visitor);
    visitor.visit(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof UnaryEval) {
      UnaryEval another = (UnaryEval) obj;
      return type == another.type && child.equals(another.child);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, child);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    UnaryEval unaryEval = (UnaryEval) super.clone();
    unaryEval.child = (EvalNode) this.child.clone();
    return unaryEval;
  }
}
