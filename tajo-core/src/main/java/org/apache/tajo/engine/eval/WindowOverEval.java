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

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.algebra.WindowSpecExpr.*;

public class WindowOverEval extends EvalNode implements Cloneable {

  @Expose private WindowFunctionEval function;

  public WindowOverEval(EvalType type) {
    super(type);
  }

  @Override
  public TajoDataTypes.DataType getValueType() {
    return function.getValueType();
  }

  @Override
  public String getName() {
    return function.getName();
  }

  @Override
  public <T extends Datum> T eval(Schema schema, Tuple tuple) {
    throw new UnsupportedOperationException("Cannot execute eval() of OVER clause");
  }

  @Override
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
    function.preOrder(visitor);
  }

  @Override
  public void postOrder(EvalNodeVisitor visitor) {
    function.preOrder(visitor);
    visitor.visit(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof WindowOverEval) {
      WindowOverEval another = (WindowOverEval) obj;
      return TUtil.checkEquals(function, another.function);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(function);
  }

  public Object clone() throws CloneNotSupportedException {
    WindowOverEval newWindowOverEval = (WindowOverEval) super.clone();
    newWindowOverEval.function = (WindowFunctionEval) function.clone();
    return newWindowOverEval;
  }
}
