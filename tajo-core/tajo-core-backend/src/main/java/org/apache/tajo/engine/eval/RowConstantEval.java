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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.ArrayDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.util.TUtil;

public class RowConstantEval extends EvalNode {
  @Expose ArrayDatum values;

  public RowConstantEval(Datum[] values) {
    super(EvalType.ROW_CONSTANT);
    this.values = new ArrayDatum(values);
  }

  @Override
  public EvalContext newContext() {
    return null;
  }

  @Override
  public TajoDataTypes.DataType[] getValueType() {
    return new TajoDataTypes.DataType[] {CatalogUtil.newDataTypeWithoutLen(values.get(0).type())};
  }

  @Override
  public String getName() {
    return "ROW";
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    return values;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RowConstantEval) {
      RowConstantEval other = (RowConstantEval) obj;
      return TUtil.checkEquals(values, other.values);
    }

    return false;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("(");
    for (int i = 0; i < values.toArray().length; i++) {
      if (i != 0) {
        sb.append(",");
      }
      sb.append(values.get(i).toString());
    }
    sb.append(")");
    return sb.toString();
  }

  public Datum [] getValues() {
    return values.toArray();
  }

  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }

  public void postOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }
}
