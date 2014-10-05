/*
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

package org.apache.tajo.engine.codegen;

import com.google.common.base.Preconditions;
import org.apache.tajo.engine.eval.BinaryEval;
import org.apache.tajo.engine.eval.CaseWhenEval;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.org.objectweb.asm.Label;
import org.apache.tajo.org.objectweb.asm.Opcodes;

import java.util.List;
import java.util.Stack;

/**
 * It generates case when byte code.
 *
 * @see org.apache.tajo.function.StaticMethodInvocationDesc
 */
class CaseWhenEmitter {

  public static void emit(EvalCodeGenerator codeGen, EvalCodeGenContext context, CaseWhenEval caseWhen,
                          Stack<EvalNode> stack) {
    // TYPE 1: CASE <expr> WHEN x THEN c1 WHEN y THEN c2 WHEN c3 ELSE ... END;
    EvalNode commonTerm = extractCommonTerm(caseWhen.getIfThenEvals());

    if (commonTerm != null) {
      int casesNum = caseWhen.getIfThenEvals().size();
      List<CaseWhenEval.IfThenEval> ifThenList = caseWhen.getIfThenEvals();
      TajoGeneratorAdapter.SwitchCase[] cases = new TajoGeneratorAdapter.SwitchCase[casesNum];

      for (int i = 0; i < caseWhen.getIfThenEvals().size(); i++) {
        int key = getSwitchIndex(ifThenList.get(i).getCondition());
        EvalNode result = ifThenList.get(i).getResult();
        cases[i] = new TajoGeneratorAdapter.SwitchCase(key, result);
      }
      CaseWhenSwitchGenerator gen = new CaseWhenSwitchGenerator(codeGen, context, stack, cases, caseWhen.getElse());

      stack.push(caseWhen);

      codeGen.visit(context, commonTerm, stack);

      Label ifNull = context.newLabel();
      Label endIf = context.newLabel();

      context.emitNullityCheck(ifNull);
      context.generatorAdapter.tableSwitch(gen.keys(), gen);
      context.gotoLabel(endIf);

      codeGen.emitLabel(context, ifNull);
      context.pop(commonTerm.getValueType());
      codeGen.visit(context, caseWhen.getElse(), stack);

      codeGen.emitLabel(context, endIf);

      stack.pop();
    } else { // TYPE 2: CASE WHEN x = c1 THEN r1 WHEN y = c2 THEN r2 ELSE ... END
      int casesNum = caseWhen.getIfThenEvals().size();
      Label [] labels = new Label[casesNum - 1];

      for (int i = 0; i < labels.length; i++) {
        labels[i] = context.newLabel();
      }

      Label defaultLabel = context.newLabel();
      Label ifNull = context.newLabel();
      Label afterAll = context.newLabel();

      // The following will generate code as follows. defaultLabel points to the last else clause.
      //
      // if (...) {
      //   .....
      // } else if (...) {
      //   ....
      // } else if (...) {
      // } ..... {
      // } else { <- default label
      //   ...
      // }

      stack.push(caseWhen);
      for (int i = 0; i < casesNum; i++) {
        CaseWhenEval.IfThenEval ifThenEval = caseWhen.getIfThenEvals().get(i);

        stack.push(caseWhen);
        codeGen.visit(context, ifThenEval.getCondition(), stack);
        int NULL_FLAG = context.istore();
        int CHILD = context.store(ifThenEval.getCondition().getValueType());

        context.iload(NULL_FLAG);
        context.emitNullityCheck(ifNull);

        context.pushBooleanOfThreeValuedLogic(true);
        context.load(ifThenEval.getCondition().getValueType(), CHILD);
        context.methodvisitor.visitJumpInsn(Opcodes.IF_ICMPNE, (casesNum - 1) < i ? labels[i] : defaultLabel);  // false

        codeGen.visit(context, ifThenEval.getResult(), stack);
        context.gotoLabel(afterAll);
        stack.pop();

        if (i < casesNum - 1) {
          codeGen.emitLabel(context, labels[i]); // else if
        }
      }
      stack.pop();

      codeGen.emitLabel(context, defaultLabel);
      if (caseWhen.hasElse()) {
        stack.push(caseWhen);
        codeGen.visit(context, caseWhen.getElse(), stack);
        stack.pop();
        context.gotoLabel(afterAll);
      } else {
        context.gotoLabel(ifNull);
      }

      codeGen.emitLabel(context, ifNull);
      context.pushDummyValue(caseWhen.getValueType());
      context.pushNullFlag(false);

      codeGen.emitLabel(context, afterAll);
    }
  }

  private static EvalNode extractCommonTerm(List<CaseWhenEval.IfThenEval> ifThenEvals) {
    EvalNode commonTerm = null;

    for (int i = 0; i < ifThenEvals.size(); i++) {
      EvalNode predicate = ifThenEvals.get(i).getCondition();
      if (!checkIfSimplePredicate(predicate)) {
        return null;
      }

      BinaryEval bin = (BinaryEval) predicate;

      EvalNode baseTerm;
      if (bin.getLeftExpr().getType() == EvalType.CONST) {
        baseTerm = bin.getRightExpr();
      } else {
        baseTerm = bin.getLeftExpr();
      }

      if (commonTerm == null) {
        commonTerm = baseTerm;
      } else {
        if (!baseTerm.equals(commonTerm)) {
          return null;
        }
      }
    }

    return commonTerm;
  }

  /**
   * Simple predicate means one term is constant and comparator is equal.
   *
   * @param predicate Predicate to be checked
   * @return True if the predicate is a simple form.
   */
  private static boolean checkIfSimplePredicate(EvalNode predicate) {
    if (predicate.getType() == EvalType.EQUAL) {
      BinaryEval binaryEval = (BinaryEval) predicate;
      EvalNode lhs = binaryEval.getLeftExpr();
      EvalNode rhs = binaryEval.getRightExpr();

      boolean simple = false;
      simple |= rhs.getType() == EvalType.CONST && TajoGeneratorAdapter.isJVMInternalInt(rhs.getValueType());
      simple |= lhs.getType() == EvalType.CONST && TajoGeneratorAdapter.isJVMInternalInt(lhs.getValueType());
      return simple;
    } else {
      return false;
    }
  }

  private static int getSwitchIndex(EvalNode predicate) {
    Preconditions.checkArgument(checkIfSimplePredicate(predicate),
        "This expression cannot be used for switch table: " + predicate);

    BinaryEval bin = (BinaryEval) predicate;

    if (bin.getLeftExpr().getType() == EvalType.CONST) {
      return bin.getLeftExpr().eval(null, null).asInt4();
    } else {
      return bin.getRightExpr().eval(null, null).asInt4();
    }
  }
}
