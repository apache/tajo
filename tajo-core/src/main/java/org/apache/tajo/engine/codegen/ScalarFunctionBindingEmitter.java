/***
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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.FunctionEval;
import org.apache.tajo.function.FunctionUtil;
import org.apache.tajo.function.StaticMethodInvocationDesc;
import org.apache.tajo.org.objectweb.asm.Label;
import org.apache.tajo.org.objectweb.asm.Opcodes;

import java.util.Stack;

/**
 * It generates the scalar function binding code for StaticMethodInvocationDesc.
 *
 * @see org.apache.tajo.function.StaticMethodInvocationDesc
 */
public class ScalarFunctionBindingEmitter {

  public static void emit(EvalCodeGenerator generator, EvalCodeGenContext context, FunctionEval func,
                          Stack<EvalNode> stack) {

    EvalNode [] params = func.getArgs();

    StaticMethodInvocationDesc method = func.getFuncDesc().getInvocation().getScalar();

    // check if there are at least one nullable field.
    int notNullParamNum = 0;
    for (Class paramClass : method.getParamClasses()) {
      notNullParamNum += !FunctionUtil.isNullableParam(paramClass) ? 1 : 0;
    }

    int nullParamFlag = -1;
    if (notNullParamNum > 0) {
      // initialize the base null flag
      context.methodvisitor.visitInsn(Opcodes.ICONST_1);
      nullParamFlag = context.istore();
    }

    stack.push(func);
    for (int paramIdx = 0; paramIdx < func.getArgs().length; paramIdx++) {
      Class clazz = method.getParamClasses()[paramIdx];

      generator.visit(context, params[paramIdx], stack);

      if (FunctionUtil.isNullableParam(clazz)) {
        emitBoxedParameter(context, func.getArgs()[paramIdx].getValueType());
      } else {
        updateNullFlag(context, clazz, nullParamFlag);
      }
    }
    stack.pop();

    if (notNullParamNum > 0) {
      Label ifNull = new Label();
      Label afterAll = new Label();

      Preconditions.checkArgument(nullParamFlag >= 0);
      context.iload(nullParamFlag);
      context.methodvisitor.visitJumpInsn(Opcodes.IFEQ, ifNull);

      // -- If all parameters are NOT NULL
      context.invokeStatic(
          method.getBaseClassName(),
          method.getMethodName(),
          method.getReturnClass(),
          method.getParamClasses());
      emitFunctionReturnValue(context, func.getValueType(), method);
      context.gotoLabel(afterAll);

      // -- If at least parameter is NULL
      context.methodvisitor.visitLabel(ifNull);

      for (int paramIdx = 0; paramIdx < func.getArgs().length; paramIdx++) {
        Class clazz = method.getParamClasses()[paramIdx];
        if (FunctionUtil.isNullableParam(clazz)) {
          context.pop();
        } else {
          context.pop(func.getArgs()[paramIdx].getValueType());
        }
      }
      context.pushDummyValue(func.getValueType());
      context.pushNullFlag(false);

      // -- After All
      context.methodvisitor.visitLabel(afterAll);

    } else {
      context.invokeStatic(
          method.getBaseClassName(),
          method.getMethodName(),
          method.getReturnClass(),
          method.getParamClasses());
      emitFunctionReturnValue(context, func.getValueType(), method);
    }
  }

  private static void emitFunctionReturnValue(EvalCodeGenContext context, TajoDataTypes.DataType returnType,
                                       StaticMethodInvocationDesc method) {
    if (FunctionUtil.isNullableParam(method.getReturnClass())) {
      Label ifNull = new Label();
      Label afterAll = new Label();

      context.dup();
      context.methodvisitor.visitJumpInsn(Opcodes.IFNULL, ifNull);

      context.emitUnboxing(context, returnType);
      context.pushNullFlag(true);        // push null flag
      context.gotoLabel(afterAll);

      context.markLabel(ifNull);
      context.pop(); // remove null reference
      context.pushDummyValue(returnType); // push dummy value for stack balance
      context.pushNullFlag(false);        // push null flag

      context.markLabel(afterAll);
    } else {
      context.pushNullFlag(true);
    }
  }

  private static void updateNullFlag(EvalCodeGenContext context, Class clazz, int nullFlagId) {
    Preconditions.checkArgument(!FunctionUtil.isNullableParam(clazz));
    context.iload(nullFlagId);
    context.methodvisitor.visitInsn(Opcodes.IAND);
    context.istore(nullFlagId);
  }

  private static void emitBoxedParameter(EvalCodeGenContext context, TajoDataTypes.DataType dataType) {
    Label ifNull = new Label();
    Label afterAll = new Label();

    context.emitNullityCheck(ifNull);

    context.emitBoxing(context, dataType);
    context.gotoLabel(afterAll);

    context.markLabel(ifNull);
    context.pop(dataType); // pop dummy value
    context.methodvisitor.visitInsn(Opcodes.ACONST_NULL);

    context.methodvisitor.visitLabel(afterAll);
  }

}
