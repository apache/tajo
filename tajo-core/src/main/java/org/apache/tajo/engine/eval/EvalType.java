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
  NOT(NotEval.class, "!"),

  // Binary expression
  AND(BinaryEval.class),
  OR(BinaryEval.class),
  EQUAL(BinaryEval.class, "="),
  IS_NULL(IsNullEval.class),
  NOT_EQUAL(BinaryEval.class, "<>"),
  LTH(BinaryEval.class, "<"),
  LEQ(BinaryEval.class, "<="),
  GTH(BinaryEval.class, ">"),
  GEQ(BinaryEval.class, ">="),
  PLUS(BinaryEval.class, "+"),
  MINUS(BinaryEval.class, "-"),
  MODULAR(BinaryEval.class, "%"),
  MULTIPLY(BinaryEval.class, "*"),
  DIVIDE(BinaryEval.class, "/"),

  // Binary Bitwise expressions
  BIT_AND(BinaryEval.class, "&"),
  BIT_OR(BinaryEval.class, "|"),
  BIT_XOR(BinaryEval.class, "|"),

  // Function
  WINDOW_FUNCTION(WindowFunctionEval.class),
  AGG_FUNCTION(AggregationFunctionCallEval.class),
  FUNCTION(GeneralFunctionEval.class),

  // String operator or pattern matching predicates
  LIKE(LikePredicateEval.class),
  SIMILAR_TO(SimilarToPredicateEval.class),
  REGEX(RegexPredicateEval.class),
  CONCATENATE(BinaryEval.class, "||"),

  // Other predicates
  BETWEEN(BetweenPredicateEval.class),
  CASE(CaseWhenEval.class),
  IF_THEN(CaseWhenEval.IfThenEval.class),
  IN(InEval.class),

  // Value or Reference
  SIGNED(SignedEval.class),
  CAST(CastEval.class),
  ROW_CONSTANT(RowConstantEval.class),
  FIELD(FieldEval.class),
  CONST(ConstEval.class);

  private Class<? extends EvalNode> baseClass;
  private String operatorName;

  EvalType(Class<? extends EvalNode> type) {
    this.baseClass = type;
  }

  EvalType(Class<? extends EvalNode> type, String text) {
    this(type);
    this.operatorName = text;
  }

  public static boolean isLogicalOperator(EvalNode evalNode) {
    EvalType type = evalNode.getType();
    boolean match = false;

    match |= type == AND;
    match |= type == OR;
    match |= type == NOT;

    return match;
  }

  public static boolean isComparisonOperator(EvalNode evalNode) {
    EvalType type = evalNode.getType();
    boolean match = false;

    match |= type == EQUAL;
    match |= type == NOT_EQUAL;
    match |= type == LTH;
    match |= type == LEQ;
    match |= type == GTH;
    match |= type == GEQ;
    match |= type == BETWEEN;

    return match;
  }

  public static boolean isArithmeticOperator(EvalNode evalNode) {
    EvalType type = evalNode.getType();
    boolean match = false;

    match |= type == PLUS;
    match |= type == MINUS;
    match |= type == MULTIPLY;
    match |= type == DIVIDE;
    match |= type == MODULAR;

    return match;
  }

  public String getOperatorName() {
    return operatorName != null ? operatorName : name();
  }

  public Class<? extends EvalNode> getBaseClass() {
    return this.baseClass;
  }
}