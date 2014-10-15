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

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.org.objectweb.asm.Opcodes;

import java.util.Stack;

class VariablesPreBuilder extends SimpleEvalNodeVisitor<EvalCodeGenContext> {

  public EvalNode visitBinaryEval(EvalCodeGenContext context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    super.visitBinaryEval(context, stack, binaryEval);

    if (EvalType.isStringPatternMatchOperator(binaryEval.getType())) {
      if (!context.symbols.containsKey(binaryEval)) {
        String fieldName = binaryEval.getType().name() + "_" + context.seqId++;
        context.symbols.put(binaryEval, fieldName);

        Class clazz = EvalCodeGenerator.getStringPatternEvalClass(binaryEval.getType());
        context.classWriter.visitField(Opcodes.ACC_PRIVATE, fieldName,
            "L" + TajoGeneratorAdapter.getInternalName(clazz) + ";", null, null);
      }
    } else if (binaryEval.getType() == EvalType.IN) {
      if (!context.symbols.containsKey(binaryEval)) {
        String fieldName = binaryEval.getType().name() + "_" + context.seqId++;
        context.symbols.put(binaryEval, fieldName);

        context.classWriter.visitField(Opcodes.ACC_PRIVATE, fieldName,
            "L" + TajoGeneratorAdapter.getInternalName(InEval.class) + ";", null, null);
      }
    }

    return binaryEval;
  }

  @Override
  public EvalNode visitConst(EvalCodeGenContext context, ConstEval constEval, Stack<EvalNode> stack) {

    if (constEval.getValueType().getType() == TajoDataTypes.Type.INTERVAL) {
      if (!context.symbols.containsKey(constEval)) {
        String fieldName = constEval.getValueType().getType().name() + "_" + context.seqId++;
        context.symbols.put(constEval, fieldName);

        context.classWriter.visitField(Opcodes.ACC_PRIVATE, fieldName,
            "L" + TajoGeneratorAdapter.getInternalName(IntervalDatum.class) + ";", null, null);
      }
    }
    return constEval;
  }

  @Override
  public EvalNode visitFuncCall(EvalCodeGenContext context, FunctionEval function, Stack<EvalNode> stack) {
    super.visitFuncCall(context, function, stack);

    if (!context.symbols.containsKey(function)) {
      String fieldName = function.getFuncDesc().getFunctionName() + "_" + context.seqId++;
      context.symbols.put(function, fieldName);
      context.classWriter.visitField(Opcodes.ACC_PRIVATE, fieldName,
          "L" + TajoGeneratorAdapter.getInternalName(function.getFuncDesc().getFuncClass()) + ";", null, null);
    }

    return function;
  }
}
