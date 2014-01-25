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

package org.apache.tajo.engine.eval;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NumericDatum;
import org.apache.tajo.storage.Tuple;

public class SignedEval extends EvalNode implements Cloneable {
  @Expose private EvalNode childEval;
  @Expose private boolean negative;

  public SignedEval(boolean negative, EvalNode childEval) {
    super(EvalType.SIGNED);
    this.negative = negative;
    this.childEval = childEval;
  }

  public boolean isNegative() {
    return negative;
  }

  public EvalNode getChild() {
    return childEval;
  }

  @Override
  public DataType getValueType() {
    return childEval.getValueType();
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  public Datum eval(Schema schema, Tuple tuple) {
    NumericDatum result = childEval.eval(schema, tuple);
    if (negative) {
      result.inverseSign();
    }
    return result;
  }

  @Override
  public String toString() {
    return (negative ? "-" : "+") + childEval.toString();
  }

  @Override
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
    childEval.preOrder(visitor);
  }

  @Override
  public void postOrder(EvalNodeVisitor visitor) {    
    childEval.postOrder(visitor);
    visitor.visit(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SignedEval) {
      SignedEval other = (SignedEval) obj;
      return negative == other.negative && this.childEval.equals(other.childEval);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SignedEval eval = (SignedEval) super.clone();
    eval.negative = negative;
    eval.childEval = (EvalNode) this.childEval.clone();
    return eval;
  }
}
