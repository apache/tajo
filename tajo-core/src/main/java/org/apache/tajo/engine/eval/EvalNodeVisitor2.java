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

import java.util.Stack;

public interface EvalNodeVisitor2<CONTEXT, RESULT> {
  RESULT visitChild(CONTEXT context, EvalNode evalNode, Stack<EvalNode> stack);

  // Column and Value reference expressions
  RESULT visitConst(CONTEXT context, ConstEval evalNode, Stack<EvalNode> stack);
  RESULT visitRowConstant(CONTEXT context, RowConstantEval evalNode, Stack<EvalNode> stack);
  RESULT visitField(CONTEXT context, Stack<EvalNode> stack, FieldEval evalNode);

  // Arithmetic expression
  RESULT visitPlus(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitMinus(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitMultiply(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitDivide(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitModular(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);

  // Logical Predicates
  RESULT visitAnd(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitOr(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitNot(CONTEXT context, NotEval evalNode, Stack<EvalNode> stack);

  // Comparison Predicates
  RESULT visitEqual(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitNotEqual(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitLessThan(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitLessThanOrEqual(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitGreaterThan(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);
  RESULT visitGreaterThanOrEqual(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);

  // Other Predicates
  RESULT visitIsNull(CONTEXT context, IsNullEval evalNode, Stack<EvalNode> stack);
  RESULT visitBetween(CONTEXT context, BetweenPredicateEval evalNode, Stack<EvalNode> stack);
  RESULT visitCaseWhen(CONTEXT context, CaseWhenEval evalNode, Stack<EvalNode> stack);
  RESULT visitIfThen(CONTEXT context, CaseWhenEval.IfThenEval evalNode, Stack<EvalNode> stack);
  RESULT visitInPredicate(CONTEXT context, InEval evalNode, Stack<EvalNode> stack);

  // String operator and Pattern matching predicates
  RESULT visitLike(CONTEXT context, LikePredicateEval evalNode, Stack<EvalNode> stack);
  RESULT visitSimilarTo(CONTEXT context, SimilarToPredicateEval evalNode, Stack<EvalNode> stack);
  RESULT visitRegex(CONTEXT context, RegexPredicateEval evalNode, Stack<EvalNode> stack);
  RESULT visitConcatenate(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack);

  // Functions
  RESULT visitFuncCall(CONTEXT context, GeneralFunctionEval evalNode, Stack<EvalNode> stack);
  RESULT visitAggrFuncCall(CONTEXT context, AggregationFunctionCallEval evalNode, Stack<EvalNode> stack);
  RESULT visitWindowFunc(CONTEXT context, WindowFunctionEval evalNode, Stack<EvalNode> stack);

  RESULT visitSigned(CONTEXT context, SignedEval signedEval, Stack<EvalNode> stack);

  RESULT visitCast(CONTEXT context, CastEval signedEval, Stack<EvalNode> stack);
}
