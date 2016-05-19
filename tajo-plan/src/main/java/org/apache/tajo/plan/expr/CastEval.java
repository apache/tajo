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

import com.google.gson.annotations.Expose;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.type.Type;

import java.util.TimeZone;

public class CastEval extends UnaryEval implements Cloneable {
  @Expose private Type target;
  private TimeZone timezone;

  public CastEval(OverridableConf context, EvalNode operand, Type target) {
    super(EvalType.CAST, operand);
    this.target = target;
  }

  public EvalNode getOperand() {
    return child;
  }

  @Override
  public Type getValueType() {
    return target;
  }

  @Override
  public String getName() {
    return target.toString();
  }

  @Override
  public EvalNode bind(@Nullable EvalContext evalContext, Schema schema) {
    if (evalContext != null) {
      timezone = evalContext.getTimeZone();
    }
    return super.bind(evalContext, schema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Datum eval(Tuple tuple) {
    super.eval(tuple);
    Datum operandDatum = child.eval(tuple);
    if (operandDatum.isNull()) {
      return operandDatum;
    }

    return DatumFactory.cast(operandDatum, target, timezone);
  }

  public String toString() {
    return "CAST (" + child + " AS " + target + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((target == null) ? 0 : target.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    boolean valid = obj != null && obj instanceof CastEval;
    if (valid) {
      CastEval another = (CastEval) obj;
      boolean b1 = child.equals(another.child);
      boolean b2 = target.equals(another.target);
      return b1 && b2;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    CastEval clone = (CastEval) super.clone();
    if (target != null) {
      clone.target = target;
    }

    return clone;
  }
}
