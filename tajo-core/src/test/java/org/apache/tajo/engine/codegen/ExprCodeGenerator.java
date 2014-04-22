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
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Stack;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class ExprCodeGenerator extends SimpleEvalNodeVisitor<ExprCodeGenerator.CodeGenContext> {

  public static final byte UNKNOWN = 0;
  public static final byte TRUE = 1;
  public static final byte FALSE = 2;

  /** 0 - UNKNOWN, 1 - TRUE, 2 - FALSE */
  @SuppressWarnings("unused")
  public static final byte [] THREE_VALUES = new byte[]  {UNKNOWN, TRUE, FALSE};
  @SuppressWarnings("unused")
  public static final byte [] NOT_LOGIC =    new byte[] {UNKNOWN, FALSE, TRUE};
  @SuppressWarnings("unused")
  public static final byte [][] AND_LOGIC = new byte [][] {
      //          unknown  true     false
      new byte [] {UNKNOWN, UNKNOWN, FALSE},   // unknown
      new byte [] {UNKNOWN, TRUE,    FALSE},   // true
      new byte [] {FALSE,   FALSE,   FALSE}    // false
  };
  @SuppressWarnings("unused")
  public static final byte [][] OR_LOGIC = new byte [][] {
      //          unknown  true     false
      new byte [] {UNKNOWN, TRUE,    UNKNOWN}, // unknown
      new byte [] {TRUE,    TRUE,    TRUE},    // true
      new byte [] {UNKNOWN, TRUE,    FALSE}    // false
  };

  public EvalNode visitBinaryEval(CodeGenContext context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    if (EvalType.isArithmeticEval(binaryEval)) {
      return visitArithmeticEval(context, binaryEval, stack);
    } else if (EvalType.isComparisonEval(binaryEval)) {
      return visitComparisonEval(context, binaryEval, stack);
    } else {
      return super.visit(context, binaryEval, stack);
    }
  }

  public EvalNode visitUnaryEval(CodeGenContext context, Stack<EvalNode> stack, UnaryEval unary) {
    stack.push(unary);
    if (unary.getType() == EvalType.CAST) {
      visit(context, unary.getChild(), stack);

      CastEval cast = (CastEval) unary;
      Type srcType = cast.getOperand().getValueType().getType();
      Type targetType = cast.getValueType().getType();
      CodeGenUtil.insertCastInst(context.evalMethod, srcType, targetType);

    } else if (unary.getType() == EvalType.NOT) {
      context.evalMethod.visitFieldInsn(Opcodes.GETSTATIC,
          CodeGenUtil.getInternalName(ExprCodeGenerator.class), "NOT_LOGIC", "[B");
      visit(context, unary.getChild(), stack);
      context.evalMethod.visitInsn(Opcodes.BALOAD);

    } else if (unary.getType() == EvalType.SIGNED) {
      visit(context, unary.getChild(), stack);
      SignedEval signed = (SignedEval) unary;
      switch (signed.getValueType().getType()) {
      case BOOLEAN:
      case CHAR:
      case INT1:
      case INT2:
      case INT4: context.evalMethod.visitInsn(Opcodes.INEG); break;
      case INT8: context.evalMethod.visitInsn(Opcodes.LNEG); break;
      case FLOAT4: context.evalMethod.visitInsn(Opcodes.FNEG); break;
      case FLOAT8: context.evalMethod.visitInsn(Opcodes.DNEG); break;
      default: throw new InvalidEvalException(unary.getType() + " operation to " + signed.getChild() + " is invalid.");
      }

    } else {
      super.visit(context, unary, stack);
    }
    stack.pop();
    return unary;
  }

  private static void invokeInitDatum(CodeGenContext context, String methodName, String paramDesc) {
    String slashedName = CodeGenUtil.getInternalName(DatumFactory.class);
    context.evalMethod.visitMethodInsn(Opcodes.INVOKESTATIC, slashedName, methodName, paramDesc);
    context.evalMethod.visitTypeInsn(Opcodes.CHECKCAST, CodeGenUtil.getInternalName(Datum.class));
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
    context.evalMethod.visitVarInsn(Opcodes.ALOAD, 0);

    String methodName;
    String signatureDesc;
    switch (expr.getValueType().getType()) {
    case BOOLEAN:
      methodName = "createBool";
      signatureDesc = "(I)L" + CodeGenUtil.getInternalName(Datum.class) +";" ;
      break;
    case INT2:
      methodName = "createInt2";
      signatureDesc = "(S)L" + CodeGenUtil.getInternalName(Int2Datum.class) +";" ;
      break;
    case INT4:
      methodName = "createInt4";
      signatureDesc = "(I)L" + CodeGenUtil.getInternalName(Int4Datum.class) +";" ;
      break;
    case INT8:
      methodName = "createInt8";
      signatureDesc = "(J)L" + CodeGenUtil.getInternalName(Int8Datum.class) +";" ;
      break;
    case FLOAT4:
      methodName = "createFloat4";
      signatureDesc = "(F)L" + CodeGenUtil.getInternalName(Float4Datum.class) +";" ;
      break;
    case FLOAT8:
      methodName = "createFloat8";
      signatureDesc = "(D)L" + CodeGenUtil.getInternalName(Float8Datum.class) +";" ;
      break;
    default:
      throw new PlanningException("Unsupported type: " + expr.getValueType().getType());
    }

    visit(context, expr, new Stack<EvalNode>());

    invokeInitDatum(context, methodName, signatureDesc);

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
    context.evalMethod.visitMethodInsn(Opcodes.INVOKEINTERFACE, CodeGenUtil.getInternalName(Tuple.class),
        methodName, desc);

    return null;
  }

  public EvalNode visitArithmeticEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);
    visit(context, evalNode.getRightExpr(), stack);
    stack.pop();

    int opCode = CodeGenUtil.getOpCode(evalNode.getType(), evalNode.getValueType());
    context.evalMethod.visitInsn(opCode);
    return evalNode;
  }

  public static boolean isJVMInternalInt(TajoDataTypes.DataType dataType) {
    return
        dataType.getType() == Type.CHAR ||
        dataType.getType() == Type.INT1 ||
        dataType.getType() == Type.INT2 ||
        dataType.getType() == Type.INT4;
  }

  public EvalNode visitComparisonEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack)
      throws CodeGenException {
    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);
    visit(context, evalNode.getRightExpr(), stack);
    stack.pop();

    Label lfalse = new Label();
    Label lafter = new Label();

    if (isJVMInternalInt(evalNode.getLeftExpr().getValueType())) {
      switch (evalNode.getType()) {
      case EQUAL:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPNE, lfalse);
        break;
      case NOT_EQUAL:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPEQ, lfalse);
        break;
      case LTH:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPGE, lfalse);
        break;
      case LEQ:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPGT, lfalse);
        break;
      case GTH:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPLE, lfalse);
        break;
      case GEQ:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPLT, lfalse);
        break;
      }

      context.evalMethod.visitInsn(Opcodes.ICONST_1); // TRUE
      context.evalMethod.visitJumpInsn(Opcodes.GOTO, lafter);
      context.evalMethod.visitLabel(lfalse);
      context.evalMethod.visitInsn(Opcodes.ICONST_2); // FALSE
      context.evalMethod.visitLabel(lafter);
    } else {
      int opCode = CodeGenUtil.getOpCode(evalNode.getType(), evalNode.getLeftExpr().getValueType());
      context.evalMethod.visitInsn(opCode);

      switch (evalNode.getType()) {
      case EQUAL:
        context.evalMethod.visitJumpInsn(Opcodes.IFNE, lfalse);
        break;
      case NOT_EQUAL:
        context.evalMethod.visitJumpInsn(Opcodes.IFEQ, lfalse);
        break;
      case LTH:
        context.evalMethod.visitJumpInsn(Opcodes.IFGE, lfalse);
        break;
      case LEQ:
        context.evalMethod.visitJumpInsn(Opcodes.IFGT, lfalse);
        break;
      case GTH:
        context.evalMethod.visitJumpInsn(Opcodes.IFLE, lfalse);
        break;
      case GEQ:
        context.evalMethod.visitJumpInsn(Opcodes.IFLT, lfalse);
        break;
      }

      context.evalMethod.visitInsn(Opcodes.ICONST_1); // TRUE
      context.evalMethod.visitJumpInsn(Opcodes.GOTO, lafter);
      context.evalMethod.visitLabel(lfalse);
      context.evalMethod.visitInsn(Opcodes.ICONST_2); // FALSE
      context.evalMethod.visitLabel(lafter);
    }

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

  public static class CodeGenContext {
    private Schema schema;

    private ClassWriter classWriter;
    private MethodVisitor evalMethod;

    public CodeGenContext(Schema schema) {
      this.schema = schema;

      classWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);
      classWriter.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test3", null, CodeGenUtil.getInternalName(EvalNode.class), null);
      classWriter.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
          null, null).visitEnd();

      // constructor method
      MethodVisitor methodVisitor = classWriter.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
      methodVisitor.visitCode();
      methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
      methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, CodeGenUtil.getInternalName(EvalNode.class), "<init>", "()V");
      methodVisitor.visitInsn(Opcodes.RETURN);
      methodVisitor.visitMaxs(1, 1);
      methodVisitor.visitEnd();
    }
  }
}
