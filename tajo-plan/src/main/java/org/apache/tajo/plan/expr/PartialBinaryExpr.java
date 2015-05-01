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

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.storage.Tuple;

public class PartialBinaryExpr extends BinaryEval {
  
  public PartialBinaryExpr(EvalType type) {
    super(type);
  }

  public PartialBinaryExpr(EvalType type, EvalNode left, EvalNode right) {
    super(type);
    this.leftExpr = left;
    this.rightExpr = right;
    // skip to determine the result type
  }

  @Override
  public DataType getValueType() {
    return null;
  }

  @Override
  public String getName() {
    return "nonamed";
  }

  @Override
  public Datum eval(Tuple tuple) {
    throw new InvalidOperationException("ERROR: the partial binary expression cannot be evaluated: "
            + this.toString());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PartialBinaryExpr) {
      return super.equals(obj);
    }
    return false;
  }

  public String toString() {
    return 
        (leftExpr != null ? leftExpr.toString() : "[EMPTY]") 
        + " " + type + " " 
        + (rightExpr != null ? rightExpr.toString() : "[EMPTY]");
  }
}
