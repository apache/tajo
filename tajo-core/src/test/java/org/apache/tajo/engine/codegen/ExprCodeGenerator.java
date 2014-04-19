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

package org.apache.tajo.engine.codegen;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.storage.Tuple;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Stack;

/**
* Created by hyunsik on 4/19/14.
*/
public class ExprCodeGenerator extends BasicEvalNodeVisitor<ExprCodeGenerator.CodeGenContext, EvalNode> {

  public EvalNode visitChild(CodeGenContext context, EvalNode evalNode, Stack<EvalNode> stack) {
    try {
      if (isArithmeticEval(evalNode)) {
        visitArithmeticOperator(context, (BinaryEval) evalNode, stack);
      } else {
        super.visitChild(context, evalNode, stack);
      }
    } catch (CodeGenException e) {
      e.printStackTrace();
    }

    return evalNode;
  }

  private static void invokeInitDatum(CodeGenContext context, Class clazz, String paramDesc) {
    String slashedName = TestExprCodeGenerator.getClassName(clazz);
    context.evalMethod.visitMethodInsn(Opcodes.INVOKESPECIAL, slashedName, "<init>", paramDesc);
    context.evalMethod.visitTypeInsn(Opcodes.CHECKCAST, TestExprCodeGenerator.getClassName(Datum.class));
    context.evalMethod.visitInsn(Opcodes.ARETURN);
    context.evalMethod.visitMaxs(0, 0);
    context.evalMethod.visitEnd();
    context.classWriter.visitEnd();
  }

  public EvalNode generate(Schema schema, EvalNode expr) throws NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, InstantiationException, PlanningException {
    CodeGenContext context = new CodeGenContext(schema);
    // evalMethod
    context.evalMethod = context.classWriter.visitMethod(Opcodes.ACC_PUBLIC, "eval",
        "(Lorg/apache/tajo/catalog/Schema;Lorg/apache/tajo/storage/Tuple;)Lorg/apache/tajo/datum/Datum;", null, null);
    context.evalMethod.visitCode();

    Class returnTypeClass;
    String signatureDesc;
    switch (expr.getValueType().getType()) {
    case INT2:
      returnTypeClass = Int2Datum.class;
      signatureDesc = "(S)V";
      break;
    case INT4:
      returnTypeClass = Int4Datum.class;
      signatureDesc = "(I)V";
      break;
    case INT8:
      returnTypeClass = Int8Datum.class;
      signatureDesc = "(J)V";
      break;
    case FLOAT4:
      returnTypeClass = Float4Datum.class;
      signatureDesc = "(F)V";
      break;
    case FLOAT8:
      returnTypeClass = Float8Datum.class;
      signatureDesc = "(D)V";
      break;
    default:
      throw new PlanningException("Unsupported type: " + expr.getValueType().getType());
    }

    context.evalMethod.visitTypeInsn(Opcodes.NEW, TestExprCodeGenerator.getClassName(returnTypeClass));
    context.evalMethod.visitInsn(Opcodes.DUP);

    visitChild(context, expr, new Stack<EvalNode>());

    invokeInitDatum(context, returnTypeClass, signatureDesc);

    TestExprCodeGenerator.MyClassLoader myClassLoader = new TestExprCodeGenerator.MyClassLoader();
    Class aClass = myClassLoader.defineClass("org.Test3", context.classWriter.toByteArray());
    Constructor constructor = aClass.getConstructor();
    EvalNode r = (EvalNode) constructor.newInstance();
    return r;
  }

  public EvalNode visitField(CodeGenContext context, Stack<EvalNode> stack, FieldEval evalNode) {
    int idx = context.schema.getColumnId(evalNode.getColumnRef().getQualifiedName());

    String methodName;
    String desc;
    switch (evalNode.getValueType().getType()) {
    case INT1:
    case INT2:
    case INT4: methodName = "getInt4"; desc = "(I)I"; break;
    case INT8: methodName = "getInt8"; desc = "(I)J"; break;
    case FLOAT4: methodName = "getFloat4"; desc = "(I)F"; break;
    case FLOAT8: methodName = "getFloat8"; desc = "(I)D"; break;
    default: throw new InvalidEvalException(evalNode.getType() + " is not supported yet");
    }

    context.evalMethod.visitVarInsn(Opcodes.ALOAD, 2);
    context.evalMethod.visitLdcInsn(idx);
    context.evalMethod.visitMethodInsn(Opcodes.INVOKEINTERFACE, TestExprCodeGenerator.getClassName(Tuple.class), methodName, desc);

    return null;
  }

  public EvalNode visitArithmeticOperator(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack)
      throws CodeGenException {
    stack.push(evalNode);
    super.visitChild(context, evalNode.getLeftExpr(), stack);
    super.visitChild(context, evalNode.getRightExpr(), stack);
    stack.pop();

    int opCode = CodeGenUtil.getOpCode(evalNode.getType(), evalNode.getValueType());
    context.evalMethod.visitInsn(opCode);
    return evalNode;
  }

  public EvalNode visitConst(CodeGenContext context, ConstEval evalNode, Stack<EvalNode> stack) {
    switch (evalNode.getValueType().getType()) {
    case INT2:
    case INT4:
      context.evalMethod.visitLdcInsn(evalNode.getValue().asInt4());
      break;
    case INT8:
      context.evalMethod.visitLdcInsn(evalNode.getValue().asInt8());
      break;
    case FLOAT4:
      context.evalMethod.visitLdcInsn(evalNode.getValue().asFloat4());
      break;
    case FLOAT8:
      context.evalMethod.visitLdcInsn(evalNode.getValue().asFloat8());
    }
    return evalNode;
  }

  @Override
  public EvalNode visitCast(CodeGenContext context, CastEval signedEval, Stack<EvalNode> stack) {
    super.visitCast(context, signedEval, stack);

    TajoDataTypes.Type srcType = signedEval.getOperand().getValueType().getType();
    TajoDataTypes.Type targetType = signedEval.getValueType().getType();
    CodeGenUtil.insertCastInst(context.evalMethod, srcType, targetType);

    return null;
  }

  public static class CodeGenContext {
    private Schema schema;

    private ClassWriter classWriter;
    private MethodVisitor evalMethod;

    public CodeGenContext(Schema schema) {
      this.schema = schema;

      classWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);
      classWriter.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test3", null, TestExprCodeGenerator.getClassName(EvalNode.class), null);
      classWriter.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
          null, null).visitEnd();

      // constructor method
      MethodVisitor methodVisitor = classWriter.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
      methodVisitor.visitCode();
      methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
      methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, TestExprCodeGenerator.getClassName(EvalNode.class), "<init>", "()V");
      methodVisitor.visitInsn(Opcodes.RETURN);
      methodVisitor.visitMaxs(1, 1);
      methodVisitor.visitEnd();
    }
  }
}
