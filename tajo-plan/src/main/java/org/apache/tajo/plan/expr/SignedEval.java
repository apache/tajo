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
import com.google.gson.annotations.Expose;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NumericDatum;
import org.apache.tajo.storage.Tuple;

public class SignedEval extends UnaryEval implements Cloneable {
  @Expose private boolean negative;

  public SignedEval(boolean negative, EvalNode childEval) {
    super(EvalType.SIGNED, childEval);
    this.negative = negative;
  }

  public boolean isNegative() {
    return negative;
  }

  @Override
  public DataType getValueType() {
    return child.getValueType();
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Datum eval(Tuple tuple) {
    super.eval(tuple);
    NumericDatum result = child.eval(tuple);
    if (negative) {
      return result.inverseSign();
    }
    return result;
  }

  @Override
  public String toString() {
    return (negative ? "-" : "+") + child.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SignedEval) {
      SignedEval other = (SignedEval) obj;
      return super.equals(other) && negative == other.negative;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, negative, child);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SignedEval signedEval = (SignedEval) super.clone();
    signedEval.negative = negative;
    return signedEval;
  }
}
