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
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.BooleanDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;

public class IsNullEval extends BinaryEval {
  // it's just a hack to emulate a binary expression
  private final static ConstEval DUMMY_EVAL = new ConstEval(DatumFactory.createBool(true));
  private static final DataType [] RES_TYPE = CatalogUtil.newDataTypesWithoutLen(TajoDataTypes.Type.BOOLEAN);

  // persistent variables
  @Expose private boolean isNot;
  @Expose private Column columnRef;
  @Expose private Integer fieldId = null;

  public IsNullEval(boolean not, FieldEval field) {
    super(EvalType.IS_NULL, field, DUMMY_EVAL);
    this.isNot = not;
    this.columnRef = field.getColumnRef();
  }

  @Override
  public EvalContext newContext() {
    return new IsNullEvalCtx();
  }

  @Override
  public DataType[] getValueType() {
    return RES_TYPE;
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    IsNullEvalCtx isNullCtx = (IsNullEvalCtx) ctx;
    if (fieldId == null) {
      fieldId = schema.getColumnId(columnRef.getQualifiedName());
    }
    if (isNot) {
      isNullCtx.result.setValue(!(tuple.get(fieldId) instanceof NullDatum));
    } else {
      isNullCtx.result.setValue(tuple.get(fieldId) instanceof NullDatum);
    }
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    return ((IsNullEvalCtx)ctx).result;
  }

  public boolean isNot() {
    return isNot;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IsNullEval) {
      IsNullEval other = (IsNullEval) obj;
      return super.equals(other) &&
          this.columnRef.equals(other.columnRef) &&
          this.fieldId == other.fieldId;
    } else {
      return false;
    }
  }

  public Object clone() throws CloneNotSupportedException {
    IsNullEval isNullEval = (IsNullEval) super.clone();
    isNullEval.columnRef = (Column) columnRef.clone();
    isNullEval.fieldId = fieldId;

    return isNullEval;
  }

  private class IsNullEvalCtx implements EvalContext {
    BooleanDatum result;

    IsNullEvalCtx() {
      this.result = DatumFactory.createBool(false);
    }
  }
}
