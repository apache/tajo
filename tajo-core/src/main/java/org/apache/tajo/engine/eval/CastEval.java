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
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class CastEval extends EvalNode {
  @Expose private EvalNode operand;
  @Expose private DataType target;

  public CastEval(EvalNode operand, DataType target) {
    super(EvalType.CAST);
    this.operand = operand;
    this.target = target;
  }

  public void setOperand(EvalNode operand) {
    this.operand = operand;
  }

  public EvalNode getOperand() {
    return operand;
  }

  @Override
  public DataType getValueType() {
    return target;
  }

  @Override
  public String getName() {
    return target.getType().name();
  }

  public Datum eval(Schema schema, Tuple tuple) {
    Datum operandDatum = operand.eval(schema, tuple);
    if (operandDatum.isNull()) {
      return operandDatum;
    }

    return DatumFactory.cast(operandDatum, target);
  }

  public String toString() {
    return "CAST (" + operand + " AS " + target.getType() + ")";
  }

  @Override
  public boolean equals(Object obj) {
    boolean valid = obj != null && obj instanceof CastEval;
    if (valid) {
      CastEval another = (CastEval) obj;
      return operand.equals(another.operand) && target.equals(another.target);
    } else {
      return false;
    }
  }

  @Override
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
    operand.preOrder(visitor);
  }

  public void postOrder(EvalNodeVisitor visitor) {
    operand.postOrder(visitor);
    visitor.visit(this);
  }
}
