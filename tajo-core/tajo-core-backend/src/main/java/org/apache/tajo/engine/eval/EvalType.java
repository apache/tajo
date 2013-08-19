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

public enum EvalType {
  // Unary expression
  NOT(NotEval.class),

  // Binary expression
  AND(BinaryEval.class),
  OR(BinaryEval.class),
  EQUAL(BinaryEval.class),
  IS_NULL(IsNullEval.class),
  NOT_EQUAL(BinaryEval.class),
  LTH(BinaryEval.class),
  LEQ(BinaryEval.class),
  GTH(BinaryEval.class),
  GEQ(BinaryEval.class),
  PLUS(BinaryEval.class),
  MINUS(BinaryEval.class),
  MODULAR(BinaryEval.class),
  MULTIPLY(BinaryEval.class),
  DIVIDE(BinaryEval.class),

  // Function
  AGG_FUNCTION(AggFuncCallEval.class),
  FUNCTION(FuncCallEval.class),

  // Predicate
  LIKE(LikeEval.class),
  CASE(CaseWhenEval.class),
  WHEN(CaseWhenEval.WhenEval.class),

  // Value or Reference
  FIELD(FieldEval.class),
  CONST(ConstEval.class);

  private Class<? extends EvalNode> baseClass;

  EvalType(Class<? extends EvalNode> type) {
    this.baseClass = type;
  }

  public Class<? extends EvalNode> getBaseClass() {
    return this.baseClass;
  }
}