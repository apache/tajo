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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;

public class NotEval extends EvalNode implements Cloneable {
  @Expose private EvalNode childEval;
  private static final DataType RES_TYPE = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.BOOLEAN);

  public NotEval(EvalNode childEval) {
    super(EvalType.NOT);
    this.childEval = childEval;
  }

  public EvalNode getChild() {
    return childEval;
  }

  @Override
  public DataType getValueType() {
    return RES_TYPE;
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  public Datum eval(Schema schema, Tuple tuple) {
    Datum datum = childEval.eval(schema, tuple);
    return !datum.isNull() ? DatumFactory.createBool(!datum.asBool()) : datum;
  }

  @Override
  public String toString() {
    return "NOT " + childEval.toString();
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
    if (obj instanceof NotEval) {
      NotEval other = (NotEval) obj;
      return this.childEval.equals(other.childEval);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    NotEval eval = (NotEval) super.clone();
    eval.childEval = (EvalNode) this.childEval.clone();
    return eval;
  }
}
