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

      Label ifNull = new Label();
      Label endIf = new Label();

      emitNullityCheck(context, ifNull);

      context.evalMethod.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(ExprCodeGenerator.class),
          "NOT_LOGIC", "[B");
      visit(context, unary.getChild(), stack);
      context.evalMethod.visitInsn(Opcodes.BALOAD);

      context.evalMethod.visitInsn(Opcodes.ICONST_1);
      emitGotoLabel(context, endIf);

      emitLabel(context, ifNull);
      context.evalMethod.visitInsn(Opcodes.ICONST_0);

      emitLabel(context, endIf);

    } else if (unary.getType() == EvalType.IS_NULL) {
      return visitIsNull(context, (IsNullEval) unary, stack);


    } else if (unary.getType() == EvalType.SIGNED) {
      visit(context, unary.getChild(), stack);

      Label ifNull = new Label();
      Label endIf = new Label();

      emitNullityCheck(context, ifNull);

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

      context.evalMethod.visitInsn(Opcodes.ICONST_1);
      emitGotoLabel(context, endIf);

      emitLabel(context, ifNull);
      context.evalMethod.visitInsn(Opcodes.ICONST_0);

      emitLabel(context, endIf);

    } else {
      super.visit(context, unary, stack);
    }
    stack.pop();
    return unary;
  }

  private void emitGotoLabel(CodeGenContext context, Label label) {
    context.evalMethod.visitJumpInsn(Opcodes.GOTO, label);
  }

  private void emitLabel(CodeGenContext context, Label label) {
    context.evalMethod.visitLabel(label);
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
      if (expr.getValueType().getLength() == 1) {
        signatureDesc = "(C)L"+ org.objectweb.asm.Type.getInternalName(CharDatum.class) + ";";
      } else {
        signatureDesc = "(L" + org.objectweb.asm.Type.getInternalName(String.class) + ";)L"
            + org.objectweb.asm.Type.getInternalName(CharDatum.class) + ";";
      }
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

    context.evalMethod.visitJumpInsn(Opcodes.IFEQ, ifNull);

    printOut(context, "generate:: NOT NULL");
    invokeInitDatum(context, methodName, signatureDesc);
    context.evalMethod.visitJumpInsn(Opcodes.GOTO, afterAll);

    context.evalMethod.visitLabel(ifNull);
    printOut(context, "generate:: NULL");
    emitPop(context, expr.getValueType());
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

  private void printOut(CodeGenContext context, String message) {
    context.evalMethod.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
    context.evalMethod.visitLdcInsn(message);
    context.evalMethod.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V");
  }

  public EvalNode visitCast(CodeGenContext context, Stack<EvalNode> stack, CastEval cast) {
    visit(context, cast.getChild(), stack);

    Label ifNull = new Label();
    Label afterEnd = new Label();
    emitNullityCheck(context, ifNull);

    TajoDataTypes.DataType  srcType = cast.getOperand().getValueType();
    TajoDataTypes.DataType targetType = cast.getValueType();
    CodeGenUtil.insertCastInst(context.evalMethod, srcType, targetType);
    context.evalMethod.visitInsn(Opcodes.ICONST_1);
    printOut(context, "endIfNotNull");
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNull);
    emitPop(context, srcType);
    emitNullTerm(context, targetType);
    context.evalMethod.visitInsn(Opcodes.ICONST_0);
    printOut(context, "endIfNull");

    emitLabel(context, afterEnd);
    return cast;
  }

  private void emitNullityCheck(CodeGenContext context, Label ifNull) {
    context.evalMethod.visitJumpInsn(Opcodes.IFEQ, ifNull);
  }

  public EvalNode visitField(CodeGenContext context, Stack<EvalNode> stack, FieldEval field) {
    int idx = context.schema.getColumnId(field.getColumnRef().getQualifiedName());

    String methodName;
    String desc;

    context.evalMethod.visitVarInsn(Opcodes.ALOAD, 2);
    context.evalMethod.visitLdcInsn(idx);
    context.evalMethod.visitMethodInsn(Opcodes.INVOKEINTERFACE, org.objectweb.asm.Type.getInternalName(Tuple.class),
        "isNull", "(I)Z");

    context.evalMethod.visitLdcInsn(true);

    Label ifNull = new Label();
    Label afterAll = new Label();
    context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPEQ, ifNull);

    switch (field.getValueType().getType()) {
    case NULL_TYPE: context.evalMethod.visitInsn(Opcodes.ICONST_0); return field;
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

    printOut(context, "visitField >> NOT NULL");
    context.evalMethod.visitVarInsn(Opcodes.ALOAD, 2);
    context.evalMethod.visitLdcInsn(idx);
    context.evalMethod.visitMethodInsn(Opcodes.INVOKEINTERFACE, org.objectweb.asm.Type.getInternalName(Tuple.class), methodName, desc);
    context.evalMethod.visitInsn(Opcodes.ICONST_1); // not null
    context.evalMethod.visitJumpInsn(Opcodes.GOTO, afterAll);

    context.evalMethod.visitLabel(ifNull);
    printOut(context, "visitField >> NULL");
    emitNullTerm(context, field.getValueType());
    context.evalMethod.visitInsn(Opcodes.ICONST_0);
    context.evalMethod.visitJumpInsn(Opcodes.GOTO, afterAll);

    context.evalMethod.visitLabel(afterAll);
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

  public static int getWordSize(TajoDataTypes.DataType type) {
    if (type.getType() == TajoDataTypes.Type.INT8 || type.getType() == TajoDataTypes.Type.FLOAT8) {
      return 2;
    } else {
      return 1;
    }
  }

  public static int emitStore(CodeGenContext context, EvalNode evalNode, int idx) {
    int nextIdx;
    switch (evalNode.getValueType().getType()) {
    case BOOLEAN:
    case CHAR:
    case INT1:
    case INT2:
    case INT4:
      context.evalMethod.visitVarInsn(Opcodes.ISTORE, idx);
      break;
    case INT8: context.evalMethod.visitVarInsn(Opcodes.LSTORE, idx); break;
    case FLOAT4: context.evalMethod.visitVarInsn(Opcodes.FSTORE, idx); break;
    case FLOAT8: context.evalMethod.visitVarInsn(Opcodes.DSTORE, idx); break;
    default: context.evalMethod.visitVarInsn(Opcodes.ASTORE, idx); break;
    }

    return idx + getWordSize(evalNode.getValueType());
  }

  public static void emitLoad(CodeGenContext context, EvalNode evalNode, int idx) {
    switch (evalNode.getValueType().getType()) {
    case BOOLEAN:
    case CHAR:
    case INT1:
    case INT2:
    case INT4:
      context.evalMethod.visitVarInsn(Opcodes.ILOAD, idx);
      break;
    case INT8:
      context.evalMethod.visitVarInsn(Opcodes.LLOAD, idx);
      break;
    case FLOAT4:
      context.evalMethod.visitVarInsn(Opcodes.FLOAD, idx);
      break;
    case FLOAT8:
      context.evalMethod.visitVarInsn(Opcodes.DLOAD, idx);
      break;
    default:
      context.evalMethod.visitVarInsn(Opcodes.ALOAD, idx);
      break;
    }
  }

  private void emitNullTerm(CodeGenContext context, TajoDataTypes.DataType type) {
    if (type.getType() == TajoDataTypes.Type.INT8) {                // < dummy_value
      context.evalMethod.visitLdcInsn(0L); // null
    } else if (type.getType() == TajoDataTypes.Type.FLOAT8) {
      context.evalMethod.visitLdcInsn(0.0d); // null
    } else if (type.getType() == TajoDataTypes.Type.FLOAT4) {
      context.evalMethod.visitLdcInsn(0.0f); // null
    } else if (type.getType() == TajoDataTypes.Type.CHAR && type.getLength() == 1) {
      context.evalMethod.visitInsn(Opcodes.ICONST_0);
    } else if (type.getType() == TajoDataTypes.Type.CHAR && type.getLength() > 1) {
      context.evalMethod.visitLdcInsn(""); // null
    } else if (type.getType() == TajoDataTypes.Type.TEXT) {
      context.evalMethod.visitLdcInsn(""); // null
    } else {
      context.evalMethod.visitInsn(Opcodes.ICONST_0);
    }
  }

  public EvalNode visitArithmeticEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);          // < left_child, push nullflag
    context.evalMethod.visitVarInsn(Opcodes.ISTORE, 3);     // < left_child
    int rNullVarId = emitStore(context, evalNode.getLeftExpr(), 4);

    visit(context, evalNode.getRightExpr(), stack);         // < left_child, right_child, nullflag
    context.evalMethod.visitVarInsn(Opcodes.ISTORE, rNullVarId);     // < left_child, right_child
    int rValVarId = rNullVarId + 1;
    emitStore(context, evalNode.getRightExpr(), rValVarId);
    stack.pop();

    Label ifNullCommon = new Label();
    Label afterEnd = new Label();

    context.evalMethod.visitVarInsn(Opcodes.ILOAD, 3);      // < left_child, right_child, nullflag
    emitNullityCheck(context, ifNullCommon);                  // < left_child, right_child

    context.evalMethod.visitVarInsn(Opcodes.ILOAD, rNullVarId);      // < left_child, right_child, nullflag
    emitNullityCheck(context, ifNullCommon);                 // < left_child, right_child

    emitLoad(context, evalNode.getLeftExpr(), 4);
    emitLoad(context, evalNode.getRightExpr(), rValVarId);

    int opCode = CodeGenUtil.getOpCode(evalNode.getType(), evalNode.getValueType());   // <
    context.evalMethod.visitInsn(opCode);                                              // < compute_value
    context.evalMethod.visitInsn(Opcodes.ICONST_1);                                    // < compute_value, nullflag
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNullCommon);
    if (evalNode.getValueType().getType() == TajoDataTypes.Type.INT8) {                // < dummy_value
      context.evalMethod.visitLdcInsn(0L); // null
    } else if (evalNode.getValueType().getType() == TajoDataTypes.Type.FLOAT8) {
      context.evalMethod.visitLdcInsn(0.0d); // null
    } else if (evalNode.getValueType().getType() == TajoDataTypes.Type.FLOAT4) {
      context.evalMethod.visitLdcInsn(0.0f); // null
    } else if (evalNode.getValueType().getType() == TajoDataTypes.Type.CHAR && evalNode.getValueType().getLength() == 1) {
      context.evalMethod.visitInsn(Opcodes.ICONST_0);
    } else if (evalNode.getValueType().getType() == TajoDataTypes.Type.CHAR && evalNode.getValueType().getLength() > 1) {
      context.evalMethod.visitLdcInsn(""); // null
    } else if (evalNode.getValueType().getType() == TajoDataTypes.Type.TEXT) {
      context.evalMethod.visitLdcInsn(""); // null
    } else {
      context.evalMethod.visitInsn(Opcodes.ICONST_0);
    }
    context.evalMethod.visitInsn(Opcodes.ICONST_0);                                     // < dummy_value, nullflag

    emitLabel(context, afterEnd);

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

    stack.push(evalNode);

    visit(context, evalNode.getLeftExpr(), stack);                    // < lhs, l_null
    context.evalMethod.visitVarInsn(Opcodes.ISTORE, 3);               // < lhs
    int rNullVarId = emitStore(context, evalNode.getLeftExpr(), 4);   // <

    visit(context, evalNode.getRightExpr(), stack);                   // < rhs, r_nullflag
    context.evalMethod.visitVarInsn(Opcodes.ISTORE, rNullVarId);      // < rhs
    int rValVarId = rNullVarId + 1;
    emitStore(context, evalNode.getRightExpr(), rValVarId);           // <
    stack.pop();

    Label ifNull = new Label();
    Label ifNotMatched = new Label();
    Label afterEnd = new Label();

    context.evalMethod.visitVarInsn(Opcodes.ILOAD, 3);                // < l_nullflag
    emitNullityCheck(context, ifNull);                          // <

    context.evalMethod.visitVarInsn(Opcodes.ILOAD, rNullVarId);       // < l_nullflag
    emitNullityCheck(context, ifNull);                          // <

    emitLoad(context, evalNode.getLeftExpr(), 4);                     // < lhs
    emitLoad(context, evalNode.getRightExpr(), rValVarId);            // < lhs, rhs


    if (isJVMInternalInt(evalNode.getLeftExpr().getValueType())) {

      switch (evalNode.getType()) {
      case EQUAL:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPNE, ifNotMatched);
        break;
      case NOT_EQUAL:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPEQ, ifNotMatched);
        break;
      case LTH:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPGE, ifNotMatched);
        break;
      case LEQ:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPGT, ifNotMatched);
        break;
      case GTH:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPLE, ifNotMatched);
        break;
      case GEQ:
        context.evalMethod.visitJumpInsn(Opcodes.IF_ICMPLT, ifNotMatched);
        break;
      }

      context.evalMethod.visitInsn(Opcodes.ICONST_1); // TRUE
      context.evalMethod.visitInsn(Opcodes.ICONST_1);
      context.evalMethod.visitJumpInsn(Opcodes.GOTO, afterEnd);

      context.evalMethod.visitLabel(ifNotMatched);
      context.evalMethod.visitInsn(Opcodes.ICONST_2); // FALSE
      context.evalMethod.visitInsn(Opcodes.ICONST_1);
      context.evalMethod.visitJumpInsn(Opcodes.GOTO, afterEnd);


    } else {

      int opCode = CodeGenUtil.getOpCode(evalNode.getType(), evalNode.getLeftExpr().getValueType());
      context.evalMethod.visitInsn(opCode);

      switch (evalNode.getType()) {
      case EQUAL:
        context.evalMethod.visitJumpInsn(Opcodes.IFNE, ifNotMatched);
        break;
      case NOT_EQUAL:
        context.evalMethod.visitJumpInsn(Opcodes.IFEQ, ifNotMatched);
        break;
      case LTH:
        context.evalMethod.visitJumpInsn(Opcodes.IFGE, ifNotMatched);
        break;
      case LEQ:
        context.evalMethod.visitJumpInsn(Opcodes.IFGT, ifNotMatched);
        break;
      case GTH:
        context.evalMethod.visitJumpInsn(Opcodes.IFLE, ifNotMatched);
        break;
      case GEQ:
        context.evalMethod.visitJumpInsn(Opcodes.IFLT, ifNotMatched);
        break;
      }

      context.evalMethod.visitInsn(Opcodes.ICONST_1); // TRUE
      context.evalMethod.visitInsn(Opcodes.ICONST_1);
      context.evalMethod.visitJumpInsn(Opcodes.GOTO, afterEnd);

      context.evalMethod.visitLabel(ifNotMatched);
      context.evalMethod.visitInsn(Opcodes.ICONST_2); // FALSE
      context.evalMethod.visitInsn(Opcodes.ICONST_1);
      context.evalMethod.visitJumpInsn(Opcodes.GOTO, afterEnd);
    }

    context.evalMethod.visitLabel(ifNull);
    context.evalMethod.visitInsn(Opcodes.ICONST_0); // NULL
    context.evalMethod.visitInsn(Opcodes.ICONST_0); // NULL

    context.evalMethod.visitLabel(afterEnd);

    return evalNode;
  }

  private static void emitPop(CodeGenContext context, TajoDataTypes.DataType type) {
    if (type.getType() == TajoDataTypes.Type.INT8 || type.getType() == TajoDataTypes.Type.FLOAT8) {
      context.evalMethod.visitInsn(Opcodes.POP2);
    } else {
      context.evalMethod.visitInsn(Opcodes.POP);
    }
  }

  public EvalNode visitIsNull(CodeGenContext context, IsNullEval isNullEval, Stack<EvalNode> stack) {

    visit(context, isNullEval.getChild(), stack);

    Label ifNull = new Label();
    Label endIf = new Label();

    emitNullityCheck(context, ifNull);

    emitPop(context, isNullEval.getChild().getValueType());
    context.evalMethod.visitInsn(isNullEval.isNot() ? Opcodes.ICONST_1 : Opcodes.ICONST_2); // TRUE
    context.evalMethod.visitJumpInsn(Opcodes.GOTO, endIf);

    context.evalMethod.visitLabel(ifNull);
    emitPop(context, isNullEval.getChild().getValueType());
    context.evalMethod.visitInsn(isNullEval.isNot() ? Opcodes.ICONST_2 : Opcodes.ICONST_1); // FALSE

    emitLabel(context, endIf);
    context.evalMethod.visitInsn(Opcodes.ICONST_1); // NOT NULL

    return isNullEval;
  }


  @Override
  public EvalNode visitConst(CodeGenContext context, ConstEval evalNode, Stack<EvalNode> stack) {
    switch (evalNode.getValueType().getType()) {
    case NULL_TYPE:
      context.evalMethod.visitInsn(Opcodes.ICONST_0); // UNKNOWN
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

    if (evalNode.getValueType().getType() == TajoDataTypes.Type.NULL_TYPE) {
      context.evalMethod.visitInsn(Opcodes.ICONST_0);
    } else {
      context.evalMethod.visitInsn(Opcodes.ICONST_1);
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
