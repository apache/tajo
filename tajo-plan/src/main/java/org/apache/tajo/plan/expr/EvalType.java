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

public enum EvalType {
  // Unary expression
  NOT(NotEval.class, "!"),
  SIGNED(SignedEval.class),

  // Binary expression
  AND(BinaryEval.class, "AND"),
  OR(BinaryEval.class, "OR"),
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
  LIKE(LikePredicateEval.class, "LIKE"),
  SIMILAR_TO(SimilarToPredicateEval.class, "SIMILAR TO"),
  REGEX(RegexPredicateEval.class, "REGEX"),
  CONCATENATE(BinaryEval.class, "||"),

  // Other predicates
  BETWEEN(BetweenPredicateEval.class),
  CASE(CaseWhenEval.class),
  IF_THEN(CaseWhenEval.IfThenEval.class),
  IN(InEval.class, "IN"),

  // Value or Reference
  CAST(CastEval.class),
  ROW_CONSTANT(RowConstantEval.class),
  FIELD(FieldEval.class),
  CONST(ConstEval.class),

  SUBQUERY(SubqueryEval.class);

  private Class<? extends EvalNode> baseClass;
  private String operatorName;

  EvalType(Class<? extends EvalNode> type) {
    this.baseClass = type;
  }

  EvalType(Class<? extends EvalNode> type, String text) {
    this(type);
    this.operatorName = text;
  }

  public static boolean isUnaryOperator(EvalType type) {
    boolean match = false;

    match |= type == CAST;
    match |= type == IS_NULL;
    match |= type == NOT;
    match |= type == SIGNED;

    return match;
  }

  public static boolean isBinaryOperator(EvalType type) {
    boolean match = false;

    match |= isArithmeticOperator(type);
    match |= isLogicalOperator(type) && type != NOT;
    match |= isComparisonOperator(type) && type != BETWEEN;

    match |= type == CONCATENATE;
    match |= type == IN;
    match |= type == LIKE;
    match |= type == REGEX;
    match |= type == SIMILAR_TO;

    return match;
  }

  public static boolean isLogicalOperator(EvalType type) {
    boolean match = false;

    match |= type == AND;
    match |= type == OR;
    match |= type == NOT;

    return match;
  }

  public static boolean isComparisonOperator(EvalType type) {
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

  public static boolean isArithmeticOperator(EvalType type) {
    boolean match = false;

    match |= type == PLUS;
    match |= type == MINUS;
    match |= type == MULTIPLY;
    match |= type == DIVIDE;
    match |= type == MODULAR;

    return match;
  }

  public static boolean isFunction(EvalType type) {
    boolean match = false;

    match |= type == FUNCTION;
    match |= type == AGG_FUNCTION;
    match |= type == WINDOW_FUNCTION;

    return match;
  }

  public static boolean isStringPatternMatchOperator(EvalType type) {
    boolean match = false;

    match |= type == LIKE;
    match |= type == SIMILAR_TO;
    match |= type == REGEX;

    return match;
  }

  public String getOperatorName() {
    return operatorName != null ? operatorName : name();
  }

  public Class<? extends EvalNode> getBaseClass() {
    return this.baseClass;
  }
}