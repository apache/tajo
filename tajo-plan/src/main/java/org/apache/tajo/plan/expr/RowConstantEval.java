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
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

import java.util.Arrays;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class RowConstantEval extends ValueSetEval {
  @Expose Datum [] values;

  public RowConstantEval(Datum [] values) {
    super(EvalType.ROW_CONSTANT);
    this.values = values;
  }

  @Override
  public DataType getValueType() {
    return CatalogUtil.newSimpleDataType(values[0].type());
  }

  @Override
  public String getName() {
    return "ROW";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Datum eval(Tuple tuple) {
    super.eval(tuple);
    return NullDatum.get();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(values);
    return result;
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
    return StringUtils.join(values);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    RowConstantEval rowConstantEval = (RowConstantEval) super.clone();
    if (values != null) {
      rowConstantEval.values = new Datum[values.length];
      System.arraycopy(values, 0, rowConstantEval.values, 0, values.length);
    }
    return rowConstantEval;
  }

  @Override
  public Datum[] getValues() {
    return values;
  }
}
