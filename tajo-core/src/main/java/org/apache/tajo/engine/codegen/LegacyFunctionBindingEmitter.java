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

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.FunctionEval;
import org.apache.tajo.org.objectweb.asm.Opcodes;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.util.Stack;

/**
 * It generates the legacy function binding code for GeneralFunction and AggFunction.
 *
 * @see org.apache.tajo.function.StaticMethodInvocationDesc
 */
public class LegacyFunctionBindingEmitter {

  public static void emit(EvalCodeGenerator generator, EvalCodeGenContext context, FunctionEval func,
                           Stack<EvalNode> stack) {
    int paramNum = func.getArgs().length;
    context.push(paramNum);
    context.newArray(Datum.class); // new Datum[paramNum]
    final int DATUM_ARRAY = context.astore();

    stack.push(func);
    EvalNode [] params = func.getArgs();
    for (int paramIdx = 0; paramIdx < func.getArgs().length; paramIdx++) {
      context.aload(DATUM_ARRAY);       // array ref
      context.methodvisitor.visitLdcInsn(paramIdx); // array idx
      generator.visit(context, params[paramIdx], stack);
      context.convertToDatum(params[paramIdx].getValueType(), true);  // value
      context.methodvisitor.visitInsn(Opcodes.AASTORE);
    }
    stack.pop();

    context.methodvisitor.visitTypeInsn(Opcodes.NEW, TajoGeneratorAdapter.getInternalName(VTuple.class));
    context.methodvisitor.visitInsn(Opcodes.DUP);
    context.aload(DATUM_ARRAY);
    context.newInstance(VTuple.class, new Class[]{Datum[].class});  // new VTuple(datum [])
    context.methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, TajoGeneratorAdapter.getInternalName(Tuple.class)); // cast to Tuple
    final int TUPLE = context.astore();

    FunctionDesc desc = func.getFuncDesc();

    String fieldName = context.symbols.get(func);
    String funcDescName = "L" + TajoGeneratorAdapter.getInternalName(desc.getFuncClass()) + ";";

    context.aload(0);
    context.methodvisitor.visitFieldInsn(Opcodes.GETFIELD, context.owner, fieldName, funcDescName);
    context.aload(TUPLE);
    context.invokeVirtual(desc.getFuncClass(), "eval", Datum.class, new Class[] {Tuple.class});

    context.convertToPrimitive(func.getValueType());
  }
}
