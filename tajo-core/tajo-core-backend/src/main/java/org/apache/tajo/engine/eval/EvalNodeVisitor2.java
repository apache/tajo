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
  RESULT visitChild(CONTEXT context, Stack<EvalNode> stack, EvalNode evalNode);

  // Column and Value reference expressions
  RESULT visitConst(CONTEXT context, Stack<EvalNode> stack, ConstEval evalNode);
  RESULT visitRowConstant(CONTEXT context, Stack<EvalNode> stack, RowConstantEval evalNode);
  RESULT visitField(CONTEXT context, Stack<EvalNode> stack, FieldEval evalNode);

  // Arithmetic expression
  RESULT visitPlus(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitMinus(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitMultiply(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitDivide(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitModular(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);

  // Logical Predicates
  RESULT visitAnd(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitOr(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitNot(CONTEXT context, Stack<EvalNode> stack, NotEval evalNode);

  // Comparison Predicates
  RESULT visitEqual(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitNotEqual(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitLessThan(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitLessThanOrEqual(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitGreaterThan(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);
  RESULT visitGreaterThanOrEqual(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode);

  // Other Predicates
  RESULT visitIsNull(CONTEXT context, Stack<EvalNode> stack, IsNullEval evalNode);
  RESULT visitCaseWhen(CONTEXT context, Stack<EvalNode> stack, CaseWhenEval evalNode);
  RESULT visitIfThen(CONTEXT context, Stack<EvalNode> stack, CaseWhenEval.IfThenEval evalNode);
  RESULT visitInPredicate(CONTEXT context, Stack<EvalNode> stack, InEval evalNode);

  // Pattern matching predicates
  RESULT visitLike(CONTEXT context, Stack<EvalNode> stack, LikePredicateEval evalNode);
  RESULT visitSimilarTo(CONTEXT context, Stack<EvalNode> stack, SimilarToPredicateEval evalNode);
  RESULT visitRegex(CONTEXT context, Stack<EvalNode> stack, RegexPredicateEval evalNode);

  // Functions
  RESULT visitFuncCall(CONTEXT context, Stack<EvalNode> stack, GeneralFunctionEval evalNode);
  RESULT visitAggrFuncCall(CONTEXT context, Stack<EvalNode> stack, AggregationFunctionCallEval evalNode);
}
