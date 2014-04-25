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

import com.sun.org.apache.bcel.internal.generic.DUP;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.storage.Tuple;
import org.apache.zookeeper.ZooDefs;
import org.mockito.asm.Type;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Stack;

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

  public static void test() {
    System.out.println(AND_LOGIC[0][1]);
  }

  public EvalNode visitBinaryEval(CodeGenContext context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    if (EvalType.isLogicalEval(binaryEval)) {
      return visitAndOrEval(context, binaryEval, stack);
    } else if (EvalType.isArithmeticEval(binaryEval)) {
      return visitArithmeticEval(context, binaryEval, stack);
    } else if (EvalType.isComparisonEval(binaryEval)) {
      return visitComparisonEval(context, binaryEval, stack);
    } else {
      stack.push(binaryEval);
      visit(context, binaryEval.getLeftExpr(), stack);
      visit(context, binaryEval.getRightExpr(), stack);
      stack.pop();
      return binaryEval;
    }
  }

  public EvalNode visitUnaryEval(CodeGenContext context, Stack<EvalNode> stack, UnaryEval unary) {
    stack.push(unary);
    if (unary.getType() == EvalType.CAST) {
      visitCast(context, stack, (CastEval) unary);


    } else if (unary.getType() == EvalType.NOT) {
      context.evalMethod.visitFieldInsn(Opcodes.GETSTATIC,
          CodeGenUtil.getInternalName(ExprCodeGenerator.class), "NOT_LOGIC", "[B");
      visit(context, unary.getChild(), stack);
      context.evalMethod.visitInsn(Opcodes.BALOAD);


    } else if (unary.getType() == EvalType.IS_NULL) {
      return visitIsNull(context, (IsNullEval) unary, stack);


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
      signatureDesc = "(I)L" + org.objectweb.asm.Type.getInternalName(Datum.class) +";" ;
      break;
    case CHAR:
      methodName = "createChar";
      signatureDesc = "(L" + org.objectweb.asm.Type.getInternalName(String.class) + ";)L"
          + org.objectweb.asm.Type.getInternalName(CharDatum.class) +";" ;
      break;
    case INT1:
    case INT2:
      methodName = "createInt2";
      signatureDesc = "(S)L" + org.objectweb.asm.Type.getInternalName(Int2Datum.class) +";" ;
      break;
    case INT4:
      methodName = "createInt4";
      signatureDesc = "(I)L" + org.objectweb.asm.Type.getInternalName(Int4Datum.class) +";" ;
      break;
    case INT8:
      methodName = "createInt8";
      signatureDesc = "(J)L" + org.objectweb.asm.Type.getInternalName(Int8Datum.class) +";" ;
      break;
    case FLOAT4:
      methodName = "createFloat4";
      signatureDesc = "(F)L" + org.objectweb.asm.Type.getInternalName(Float4Datum.class) +";" ;
      break;
    case FLOAT8:
      methodName = "createFloat8";
      signatureDesc = "(D)L" + org.objectweb.asm.Type.getInternalName(Float8Datum.class) +";" ;
      break;
    case TEXT:
      methodName = "createText";
      signatureDesc = "(L" + org.objectweb.asm.Type.getInternalName(String.class) + ";)L"
          + org.objectweb.asm.Type.getInternalName(TextDatum.class) +";" ;
      break;
    default:
      throw new PlanningException("Unsupported type: " + expr.getValueType().getType());
    }

    Label ifNull = new Label();
    Label afterAll = new Label();

    visit(context, expr, new Stack<EvalNode>());

//    context.evalMethod.visitJumpInsn(Opcodes.IFEQ, ifNull);

    printOut(context.evalMethod, "generate:: NOT NULL");
    invokeInitDatum(context, methodName, signatureDesc);
    context.evalMethod.visitJumpInsn(Opcodes.GOTO, afterAll);

    context.evalMethod.visitLabel(ifNull);
    printOut(context.evalMethod, "NULL");
    context.evalMethod.visitInsn(Opcodes.POP);
    context.evalMethod.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(NullDatum.class), "get",
        "()L" + Type.getInternalName(NullDatum.class) + ";");

    context.evalMethod.visitLabel(afterAll);
    context.evalMethod.visitTypeInsn(Opcodes.CHECKCAST, CodeGenUtil.getInternalName(Datum.class));
    context.evalMethod.visitInsn(Opcodes.ARETURN);
    context.evalMethod.visitMaxs(0, 0);
    context.evalMethod.visitEnd();
    context.classWriter.visitEnd();

    TestExprCodeGenerator.MyClassLoader myClassLoader = new TestExprCodeGenerator.MyClassLoader();
    Class aClass = myClassLoader.defineClass("org.Test3", context.classWriter.toByteArray());
    Constructor constructor = aClass.getConstructor();
    EvalNode r = (EvalNode) constructor.newInstance();
    return r;
  }

  private void printOut(MethodVisitor methodVisitor, String message) {
    methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
    methodVisitor.visitLdcInsn(message);
    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V");
  }

  public EvalNode visitCast(CodeGenContext context, Stack<EvalNode> stack, CastEval cast) {
    visit(context, cast.getChild(), stack);
    TajoDataTypes.DataType  srcType = cast.getOperand().getValueType();
    TajoDataTypes.DataType targetType = cast.getValueType();
    CodeGenUtil.insertCastInst(context.evalMethod, srcType, targetType);

    return cast;
  }

  public EvalNode visitField(CodeGenContext context, Stack<EvalNode> stack, FieldEval field) {
    int idx = context.schema.getColumnId(field.getColumnRef().getQualifiedName());

    String methodName;
    String desc;

    context.evalMethod.visitVarInsn(Opcodes.ALOAD, 2);
    context.evalMethod.visitLdcInsn(idx);
    context.evalMethod.visitMethodInsn(Opcodes.INVOKEINTERFACE, org.objectweb.asm.Type.getInternalName(Tuple.class),
        "isNull", "(I)Z");
    context.evalMethod.visitInsn(Opcodes.POP);

//    context.evalMethod.visitLdcInsn(true);

//    Label ifNull = new Label();
//    Label afterAll = new Label();
//    context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPEQ, ifNull);

    switch (field.getValueType().getType()) {
    case NULL_TYPE: context.evalMethod.visitInsn(Opcodes.ACONST_NULL); return field;
    case BOOLEAN: methodName = "getByte"; desc = "(I)B"; break;
    case CHAR: {
      if (field.getValueType().hasLength() && field.getValueType().getLength() == 1) {
        methodName = "getChar"; desc = "(I)C";
      } else {
        methodName = "getText"; desc = "(I)L" + org.objectweb.asm.Type.getInternalName(String.class)+";";
      }
      break;
    }
    case INT1:
    case INT2:
    case INT4: methodName = "getInt4"; desc = "(I)I"; break;
    case INT8: methodName = "getInt8"; desc = "(I)J"; break;
    case FLOAT4: methodName = "getFloat4"; desc = "(I)F"; break;
    case FLOAT8: methodName = "getFloat8"; desc = "(I)D"; break;
    case TEXT: methodName = "getText"; desc = "(I)L" + org.objectweb.asm.Type.getInternalName(String.class)+";"; break;
    case TIMESTAMP: methodName = "getTimestamp"; desc = "(I)L" + org.objectweb.asm.Type.getInternalName(Datum.class)+";"; break;
    default: throw new InvalidEvalException(field.getValueType() + " is not supported yet");
    }

//    printOut(context.evalMethod, "visitField >> NOT NULL");
    context.evalMethod.visitVarInsn(Opcodes.ALOAD, 2);
    context.evalMethod.visitLdcInsn(idx);
    context.evalMethod.visitMethodInsn(Opcodes.INVOKEINTERFACE, org.objectweb.asm.Type.getInternalName(Tuple.class), methodName, desc);
//    context.evalMethod.visitLdcInsn(1); // not null
//    context.evalMethod.visitJumpInsn(Opcodes.GOTO, afterAll);

//    context.evalMethod.visitLabel(ifNull);
//    printOut(context.evalMethod, "visitField >> NULL");
//    context.evalMethod.visitInsn(Opcodes.ICONST_0); // null
//    context.evalMethod.visitInsn(Opcodes.ICONST_0);

//    context.evalMethod.visitLabel(afterAll);
    return field;
  }

  public EvalNode visitAndOrEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
    if (evalNode.getType() == EvalType.AND) {
      context.evalMethod.visitFieldInsn(Opcodes.GETSTATIC,
          CodeGenUtil.getInternalName(ExprCodeGenerator.class), "AND_LOGIC", "[[B");
    } else if (evalNode.getType() == EvalType.OR) {
      context.evalMethod.visitFieldInsn(Opcodes.GETSTATIC,
          CodeGenUtil.getInternalName(ExprCodeGenerator.class), "OR_LOGIC", "[[B");
    } else {
      throw new CodeGenException("visitAndOrEval() cannot generate the code at " + evalNode);
    }

    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);
    context.evalMethod.visitInsn(Opcodes.AALOAD);
    visit(context, evalNode.getRightExpr(), stack);
    context.evalMethod.visitInsn(Opcodes.BALOAD);
    stack.pop();

    return evalNode;
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
        dataType.getType() == TajoDataTypes.Type.CHAR ||
        dataType.getType() == TajoDataTypes.Type.INT1 ||
        dataType.getType() == TajoDataTypes.Type.INT2 ||
        dataType.getType() == TajoDataTypes.Type.INT4;
  }

  public EvalNode visitComparisonEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack)
      throws CodeGenException {
    Label ifNull = new Label();

    stack.push(evalNode);

    visit(context, evalNode.getLeftExpr(), stack);
//    context.evalMethod.visitInsn(Opcodes.DUP);
//    context.evalMethod.visitInsn(Opcodes.POP);
//    context.evalMethod.visitJumpInsn(Opcodes.IFNULL, ifNull);

    visit(context, evalNode.getRightExpr(), stack);
//    context.evalMethod.visitInsn(Opcodes.DUP);
//    context.evalMethod.visitJumpInsn(Opcodes.IFNULL, ifNull);

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
      context.evalMethod.visitJumpInsn(Opcodes.GOTO, lafter);


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
      context.evalMethod.visitJumpInsn(Opcodes.GOTO, lafter);
    }

    context.evalMethod.visitLabel(ifNull);
    context.evalMethod.visitInsn(Opcodes.ICONST_0); // NULL
    context.evalMethod.visitJumpInsn(Opcodes.GOTO, lafter);

    context.evalMethod.visitLabel(lafter);

    return evalNode;
  }

  public EvalNode visitIsNull(CodeGenContext context, IsNullEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    super.visit(context, evalNode.getChild(), stack);
    stack.pop();

    Label lfalse = new Label();
    Label lafter = new Label();

    if (evalNode.getChild().getValueType().getType() == TajoDataTypes.Type.BOOLEAN) {
      if (evalNode.isNot()) {

      } else {

      }
    } else {
      if (evalNode.isNot()) {
        context.evalMethod.visitJumpInsn(Opcodes.IFNULL, lfalse);
      } else {
        context.evalMethod.visitJumpInsn(Opcodes.IFNONNULL, lfalse);
      }

      context.evalMethod.visitInsn(Opcodes.ICONST_1); // TRUE
      context.evalMethod.visitJumpInsn(Opcodes.GOTO, lafter);
      context.evalMethod.visitLabel(lfalse);
      context.evalMethod.visitInsn(Opcodes.ICONST_2); // FALSE
      context.evalMethod.visitLabel(lafter);
    }

    return evalNode;
  }


  @Override
  public EvalNode visitConst(CodeGenContext context, ConstEval evalNode, Stack<EvalNode> stack) {
    switch (evalNode.getValueType().getType()) {
    case NULL_TYPE:
      context.evalMethod.visitInsn(Opcodes.ACONST_NULL); // UNKNOWN
      break;
    case BOOLEAN:
      if (evalNode.getValue().asInt2() == 1) {
        context.evalMethod.visitInsn(Opcodes.ICONST_1); // TRUE
      } else if (evalNode.getValue().asInt4() == 2) {
        context.evalMethod.visitInsn(Opcodes.ICONST_2); // FALSE
      } else {
        context.evalMethod.visitInsn(Opcodes.ICONST_0); // UNKNOWN
      }
      break;
    case INT1:
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
      break;
    case TEXT:
      context.evalMethod.visitLdcInsn(evalNode.getValue().asChars());
      break;
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
