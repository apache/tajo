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

  public EvalNode getOperand() {
    return operand;
  }

  @Override
  public EvalContext newContext() {
    CastContext castContext = new CastContext();
    castContext.childCtx = operand.newContext();
    return castContext;
  }

  @Override
  public DataType getValueType() {
    return target;
  }

  @Override
  public String getName() {
    return target.getType().name();
  }

  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    CastContext castContext = (CastContext) ctx;
    operand.eval(castContext.childCtx , schema, tuple);
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    CastContext castContext = (CastContext) ctx;
    Datum operandDatum = operand.terminate(castContext.childCtx);
    if (operandDatum.isNull()) {
      return operandDatum;
    }

    switch (target.getType()) {
      case BOOLEAN:
        return DatumFactory.createBool(operandDatum.asBool());
      case CHAR:
        return DatumFactory.createChar(operandDatum.asChar());
      case INT1:
      case INT2:
        return DatumFactory.createInt2(operandDatum.asInt2());
      case INT4:
        return DatumFactory.createInt4(operandDatum.asInt4());
      case INT8:
        return DatumFactory.createInt8(operandDatum.asInt8());
      case FLOAT4:
        return DatumFactory.createFloat4(operandDatum.asFloat4());
      case FLOAT8:
        return DatumFactory.createFloat8(operandDatum.asFloat8());
      case TEXT:
        return DatumFactory.createText(operandDatum.asTextBytes());
      case DATE:
        return DatumFactory.createDate(operandDatum);
      case TIME:
        return DatumFactory.createTime(operandDatum);
      case TIMESTAMP:
        return DatumFactory.createTimestamp(operandDatum);
      case BLOB:
        return DatumFactory.createBlob(operandDatum.asByteArray());
      default:
        throw new InvalidCastException("Cannot cast " + operand.getValueType().getType() + " to "
            + target.getType());
    }
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

  static class CastContext implements EvalContext {
    EvalContext childCtx;
  }
}
