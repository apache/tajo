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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.mockito.asm.Type;
import org.objectweb.asm.*;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.engine.codegen.TajoGeneratorAdapter.SwitchCase;
import static org.apache.tajo.engine.codegen.TajoGeneratorAdapter.SwitchCaseGenerator;
import static org.apache.tajo.engine.eval.FunctionEval.ParamType;

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
    if (EvalType.isLogicalOperator(binaryEval)) {
      return visitAndOrEval(context, binaryEval, stack);
    } else if (EvalType.isArithmeticOperator(binaryEval)) {
      return visitArithmeticEval(context, binaryEval, stack);
    } else if (EvalType.isComparisonOperator(binaryEval)) {
      return visitComparisonEval(context, binaryEval, stack);
    } else if (binaryEval.getType() == EvalType.CONCATENATE) {
      return visitStringConcat(context, binaryEval, stack);
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

      visit(context, unary.getChild(), stack);
      context.methodvisitor.visitVarInsn(Opcodes.ISTORE, 9);
      context.methodvisitor.visitVarInsn(Opcodes.ISTORE, 10);

      Label ifNull = new Label();
      Label endIf = new Label();

      context.emitNullityCheck(ifNull, 9);

      context.methodvisitor.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(ExprCodeGenerator.class),
          "NOT_LOGIC", "[B");
      context.methodvisitor.visitVarInsn(Opcodes.ILOAD, 10);
      context.methodvisitor.visitInsn(Opcodes.BALOAD);
      context.pushNullFlag(true);
      emitGotoLabel(context, endIf);

      emitLabel(context, ifNull);
      context.pushDummyValue(unary.getValueType());
      context.pushNullFlag(false);

      emitLabel(context, endIf);

    } else if (unary.getType() == EvalType.IS_NULL) {
      return visitIsNull(context, (IsNullEval) unary, stack);


    } else if (unary.getType() == EvalType.SIGNED) {
      visit(context, unary.getChild(), stack);

      Label ifNull = new Label();
      Label endIf = new Label();

      context.emitNullityCheck(ifNull);

      SignedEval signed = (SignedEval) unary;
      switch (signed.getValueType().getType()) {
      case BOOLEAN:
      case CHAR:
      case INT1:
      case INT2:
      case INT4: context.methodvisitor.visitInsn(Opcodes.INEG); break;
      case INT8: context.methodvisitor.visitInsn(Opcodes.LNEG); break;
      case FLOAT4: context.methodvisitor.visitInsn(Opcodes.FNEG); break;
      case FLOAT8: context.methodvisitor.visitInsn(Opcodes.DNEG); break;
      default: throw new InvalidEvalException(unary.getType() + " operation to " + signed.getChild() + " is invalid.");
      }

      context.pushNullFlag(true);
      emitGotoLabel(context, endIf);

      emitLabel(context, ifNull);
      context.pushNullFlag(false);

      emitLabel(context, endIf);

    } else {
      super.visit(context, unary, stack);
    }
    stack.pop();
    return unary;
  }

  public EvalNode visitBetween(CodeGenContext context, BetweenPredicateEval between, Stack<EvalNode> stack) {
    EvalNode predicand = between.getPredicand();
    EvalNode begin = between.getBegin();
    EvalNode end = between.getEnd();

    stack.push(between);

    int predNullVarId = 3;
    int predVarId = 4;
    visit(context, predicand, stack);                                 // < predicand, predicand_nullflag
    context.methodvisitor.visitVarInsn(Opcodes.ISTORE, predNullVarId);      // < predicand (store nullflag to 3)
    int beginNullVarId = store(context, predicand.getValueType(), predVarId);    // <

    visit(context, begin, stack);                                    // < begin, left_nullflag
    context.methodvisitor.visitVarInsn(Opcodes.ISTORE, beginNullVarId);  // < begin, store left_nullflag to x
    int beginVarId = beginNullVarId + 1;
    int endNullVarId = store(context, begin.getValueType(), beginVarId);

    visit(context, end, stack);                                         // < end, right_nullflag
    context.methodvisitor.visitVarInsn(Opcodes.ISTORE, endNullVarId);      // < end, store right_nullflag
    int endVarId = endNullVarId + 1;
    store(context, end.getValueType(), endVarId);                                // <

    stack.pop();

    Label ifNullCommon = new Label();
    Label ifNotMatched = new Label();

    Label afterEnd = new Label();


    context.emitNullityCheck(ifNullCommon, predNullVarId, beginNullVarId, endNullVarId);

    if (between.isSymmetric()) {
      Label ifFirstMatchFailed = new Label();
      Label ifSecondMatchFailed = new Label();
      Label secondCheck = new Label();
      Label finalDisjunctive = new Label();

      //////////////////////////////////////////////////////////////////////////////////////////
      // second check
      //////////////////////////////////////////////////////////////////////////////////////////

      // predicand <= begin
      context.load(begin.getValueType(), beginVarId);
      context.load(predicand.getValueType(), predVarId);
      context.ifCmp(predicand.getValueType(), EvalType.LEQ, ifFirstMatchFailed);

      // end <= predicand
      context.load(end.getValueType(), endVarId);
      context.load(predicand.getValueType(), predVarId);
      // inverse the operator GEQ -> LTH
      context.ifCmp(predicand.getValueType(), EvalType.GEQ, ifFirstMatchFailed);

      context.push(true);
      emitGotoLabel(context, secondCheck);

      emitLabel(context, ifFirstMatchFailed);
      context.push(false);

      //////////////////////////////////////////////////////////////////////////////////////////
      // second check
      //////////////////////////////////////////////////////////////////////////////////////////
      emitLabel(context, secondCheck);

      // predicand <= end
      context.load(end.getValueType(), endVarId);
      context.load(predicand.getValueType(), predVarId);

      // inverse the operator LEQ -> GTH
      context.ifCmp(predicand.getValueType(), EvalType.LEQ, ifSecondMatchFailed);

      // end <= predicand
      context.load(begin.getValueType(), beginVarId);
      context.load(predicand.getValueType(), predVarId);
      // inverse the operator GEQ -> LTH
      context.ifCmp(predicand.getValueType(), EvalType.GEQ, ifSecondMatchFailed);

      context.push(true);
      emitGotoLabel(context, finalDisjunctive);

      emitLabel(context, ifSecondMatchFailed);
      context.push(false);

      emitLabel(context, finalDisjunctive);
      context.methodvisitor.visitInsn(Opcodes.IOR);
      context.methodvisitor.visitJumpInsn(Opcodes.IFEQ, ifNotMatched);
    } else {
      // predicand <= begin
      context.load(begin.getValueType(), beginVarId);
      context.load(predicand.getValueType(), predVarId);
      context.ifCmp(predicand.getValueType(), EvalType.LEQ, ifNotMatched);

      // end <= predicand
      context.load(end.getValueType(), endVarId);
      context.load(predicand.getValueType(), predVarId);
      context.ifCmp(predicand.getValueType(), EvalType.GEQ, ifNotMatched);
    }

    // IF MATCHED
    context.pushBooleanOfThreeValuedLogic(between.isNot() ? false : true);
    context.pushNullFlag(true);
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNotMatched); // IF NOT MATCHED
    context.pushBooleanOfThreeValuedLogic(between.isNot() ? true : false);
    context.pushNullFlag(true);
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNullCommon); // IF NULL
    context.pushNullOfThreeValuedLogic();
    context.pushNullFlag(false);

    emitLabel(context, afterEnd);

    return between;
  }

  private void emitGotoLabel(CodeGenContext context, Label label) {
    context.methodvisitor.visitJumpInsn(Opcodes.GOTO, label);
  }

  private void emitLabel(CodeGenContext context, Label label) {
    context.methodvisitor.visitLabel(label);
  }

  public EvalNode generate(Schema schema, EvalNode expr) throws NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, InstantiationException, PlanningException {

    ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);

    classWriter.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/apache/tajo/Test3", null,
        TajoGeneratorAdapter.getInternalName(EvalNode.class), null);
    classWriter.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;", null, null).visitEnd();

    // constructor method
    MethodVisitor initMV = classWriter.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    initMV.visitCode();
    initMV.visitVarInsn(Opcodes.ALOAD, 0);
    initMV.visitMethodInsn(Opcodes.INVOKESPECIAL, TajoGeneratorAdapter.getInternalName(EvalNode.class), "<init>",
        "()V");
    initMV.visitInsn(Opcodes.RETURN);
    initMV.visitMaxs(1, 1);
    initMV.visitEnd();

    String methodName = "eval";
    String methodDesc = TajoGeneratorAdapter.getMethodDescription(Datum.class, new Class[]{Schema.class, Tuple.class});
    // method
    MethodVisitor evalMV = classWriter.visitMethod(Opcodes.ACC_PUBLIC, methodName, methodDesc, null, null);
    evalMV.visitCode();

    CodeGenContext context = new CodeGenContext(schema, Opcodes.ACC_PUBLIC, evalMV, methodName, methodDesc);

    visit(context, expr, new Stack<EvalNode>());

    context.convertToDatum(expr.getValueType(), true);
    context.methodvisitor.visitInsn(Opcodes.ARETURN);
    context.methodvisitor.visitMaxs(0, 0);
    context.methodvisitor.visitEnd();
    classWriter.visitEnd();

    TestExprCodeGenerator.MyClassLoader myClassLoader = new TestExprCodeGenerator.MyClassLoader();
    Class aClass = myClassLoader.defineClass("org.apache.tajo.Test3", classWriter.toByteArray());
    Constructor constructor = aClass.getConstructor();
    EvalNode r = (EvalNode) constructor.newInstance();
    return r;
  }

  private void printOut(CodeGenContext context, String message) {
    context.methodvisitor.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
    context.push(message);
    context.invokeVirtual(PrintStream.class, "println", void.class, new Class[]{String.class});
  }

  public EvalNode visitCast(CodeGenContext context, Stack<EvalNode> stack, CastEval cast) {
    DataType  srcType = cast.getOperand().getValueType();
    DataType targetType = cast.getValueType();

    if (srcType.equals(targetType)) {
      visit(context, cast.getChild(), stack);
      return cast;
    }

    visit(context, cast.getChild(), stack);

    Label ifNull = new Label();
    Label afterEnd = new Label();
    context.emitNullityCheck(ifNull);

    context.castInsn(srcType, targetType);
    context.pushNullFlag(true);
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNull);
    context.emitPop(srcType);
    context.pushDummyValue(targetType);
    context.pushNullFlag(false);
    printOut(context, "endIfNull");

    emitLabel(context, afterEnd);
    return cast;
  }

  public EvalNode visitField(CodeGenContext context, Stack<EvalNode> stack, FieldEval field) {
    printOut(context, "enter visitField");

    if (field.getValueType().getType() == TajoDataTypes.Type.NULL_TYPE) {
      printOut(context, "visitField >> NULL");
      context.pushNullOfThreeValuedLogic();
      context.pushNullFlag(false);
    } else {
      String methodName;
      int idx = context.schema.getColumnId(field.getColumnRef().getQualifiedName());

      context.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
      context.push(idx);
      context.invokeInterface(Tuple.class, "isNull", boolean.class, new Class [] {int.class});

      context.push(true);

      Label ifNull = new Label();
      Label afterAll = new Label();
      context.methodvisitor.visitJumpInsn(Opcodes.IF_ICMPEQ, ifNull);

      Class returnType;
      Class [] paramTypes;
      switch (field.getValueType().getType()) {
      case BOOLEAN:
        methodName = "getByte";
        returnType = byte.class;
        paramTypes = new Class[] {int.class};
        break;
      case CHAR: {
        methodName = "getText";
        returnType = String.class;
        paramTypes = new Class[] {int.class};
        break;
      }
      case INT1:
      case INT2:
      case INT4:
        methodName = "getInt4";
        returnType = int.class;
        paramTypes = new Class [] {int.class};
        break;
      case INT8:
        methodName = "getInt8";
        returnType = long.class;
        paramTypes = new Class [] {int.class};
        break;
      case FLOAT4:
        methodName = "getFloat4";
        returnType = float.class;
        paramTypes = new Class [] {int.class};
        break;
      case FLOAT8:
        methodName = "getFloat8";
        returnType = double.class;
        paramTypes = new Class [] {int.class};
        break;
      case TEXT:
        methodName = "getText";
        returnType = String.class;
        paramTypes = new Class [] {int.class};
        break;
      case TIMESTAMP:
        methodName = "getTimestamp";
        returnType = TimestampDatum.class;
        paramTypes = new Class [] {int.class};
        break;
      default:
        throw new InvalidEvalException(field.getValueType() + " is not supported yet");
      }

      context.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
      context.push(idx);
      context.invokeInterface(Tuple.class, methodName, returnType, paramTypes);

      context.pushNullFlag(true); // not null
      context.methodvisitor.visitJumpInsn(Opcodes.GOTO, afterAll);

      context.methodvisitor.visitLabel(ifNull);
      context.pushDummyValue(field.getValueType());
      context.pushNullFlag(false);

      context.methodvisitor.visitLabel(afterAll);
    }
    return field;
  }

  public EvalNode visitAndOrEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {

    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);
    context.pop();
    int LHS = context.istore();

    visit(context, evalNode.getRightExpr(), stack);
    context.pop();
    int RHS = context.istore();
    stack.pop();

    if (evalNode.getType() == EvalType.AND) {
      context.methodvisitor.visitFieldInsn(Opcodes.GETSTATIC,
          org.objectweb.asm.Type.getInternalName(ExprCodeGenerator.class), "AND_LOGIC", "[[B");
    } else if (evalNode.getType() == EvalType.OR) {
      context.methodvisitor.visitFieldInsn(Opcodes.GETSTATIC,
          org.objectweb.asm.Type.getInternalName(ExprCodeGenerator.class), "OR_LOGIC", "[[B");
    } else {
      throw new CodeGenException("visitAndOrEval() cannot generate the code at " + evalNode);
    }
    context.load(evalNode.getLeftExpr().getValueType(), LHS);
    context.methodvisitor.visitInsn(Opcodes.AALOAD);
    context.load(evalNode.getRightExpr().getValueType(), RHS);
    context.methodvisitor.visitInsn(Opcodes.BALOAD);    // get three valued logic number from the AND/OR_LOGIC array
    context.methodvisitor.visitInsn(Opcodes.DUP); // three valued logic number x 2, three valued logic number can be null flag.

    return evalNode;
  }

  public static int store(CodeGenContext context, DataType type, int idx) {
    switch (type.getType()) {
    case NULL_TYPE:
    case BOOLEAN:
    case CHAR:
    case INT1:
    case INT2:
    case INT4:
      context.methodvisitor.visitVarInsn(Opcodes.ISTORE, idx);
      break;
    case INT8: context.methodvisitor.visitVarInsn(Opcodes.LSTORE, idx); break;
    case FLOAT4: context.methodvisitor.visitVarInsn(Opcodes.FSTORE, idx); break;
    case FLOAT8: context.methodvisitor.visitVarInsn(Opcodes.DSTORE, idx); break;
    default: context.methodvisitor.visitVarInsn(Opcodes.ASTORE, idx); break;
    }

    return idx + TajoGeneratorAdapter.getWordSize(type);
  }

  public EvalNode visitArithmeticEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);          // < left_child, push nullflag
    context.methodvisitor.visitVarInsn(Opcodes.ISTORE, 3);     // < left_child
    int rNullVarId = store(context, evalNode.getLeftExpr().getValueType(), 4);

    visit(context, evalNode.getRightExpr(), stack);         // < left_child, right_child, nullflag
    context.methodvisitor.visitVarInsn(Opcodes.ISTORE, rNullVarId);     // < left_child, right_child
    int rValVarId = rNullVarId + 1;
    store(context, evalNode.getRightExpr().getValueType(), rValVarId);
    stack.pop();

    Label ifNullCommon = new Label();
    Label afterEnd = new Label();

    context.emitNullityCheck(ifNullCommon, 3, rNullVarId);

    context.load(evalNode.getLeftExpr().getValueType(), 4);
    context.load(evalNode.getRightExpr().getValueType(), rValVarId);

    int opCode = TajoGeneratorAdapter.getOpCode(evalNode.getType(), evalNode.getValueType());
    context.methodvisitor.visitInsn(opCode);

    context.pushNullFlag(true);
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNullCommon);
    context.pushDummyValue(evalNode.getValueType());
    context.pushNullFlag(false);

    emitLabel(context, afterEnd);

    return evalNode;
  }

  public EvalNode visitComparisonEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack)
      throws CodeGenException {

    DataType lhsType = evalNode.getLeftExpr().getValueType();
    DataType rhsType = evalNode.getRightExpr().getValueType();

    if (lhsType.getType() == TajoDataTypes.Type.NULL_TYPE || rhsType.getType() == TajoDataTypes.Type.NULL_TYPE) {
      context.pushNullOfThreeValuedLogic();
      context.pushNullFlag(false);
    } else {
      stack.push(evalNode);
      visit(context, evalNode.getLeftExpr(), stack);                    // < lhs, l_null
      final int LHS_NULLFLAG = context.istore();
      int LHS = context.store(evalNode.getLeftExpr().getValueType());   // <

      visit(context, evalNode.getRightExpr(), stack);                   // < rhs, r_nullflag
      final int RHS_NULLFLAG = context.istore();
      final int RHS = context.store(evalNode.getRightExpr().getValueType());           // <
      stack.pop();

      Label ifNull = new Label();
      Label ifNotMatched = new Label();
      Label afterEnd = new Label();

      context.emitNullityCheck(ifNull, LHS_NULLFLAG, RHS_NULLFLAG);

      context.load(evalNode.getLeftExpr().getValueType(), LHS);                     // < lhs
      context.load(evalNode.getRightExpr().getValueType(), RHS);            // < lhs, rhs

      context.ifCmp(evalNode.getLeftExpr().getValueType(), evalNode.getType(), ifNotMatched);

      context.pushBooleanOfThreeValuedLogic(true);
      context.pushNullFlag(true);
      context.methodvisitor.visitJumpInsn(Opcodes.GOTO, afterEnd);

      context.methodvisitor.visitLabel(ifNotMatched);
      context.pushBooleanOfThreeValuedLogic(false);
      context.pushNullFlag(true);
      context.methodvisitor.visitJumpInsn(Opcodes.GOTO, afterEnd);

      context.methodvisitor.visitLabel(ifNull);
      context.pushNullOfThreeValuedLogic();
      context.pushNullFlag(false);

      context.methodvisitor.visitLabel(afterEnd);
    }

    return evalNode;
  }

  public EvalNode visitStringConcat(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack)
      throws CodeGenException {

    stack.push(evalNode);

    visit(context, evalNode.getLeftExpr(), stack);                    // < lhs, l_null
    final int LHS_NULLFLAG = context.istore();               // < lhs
    final int LHS = context.store(evalNode.getLeftExpr().getValueType());

    visit(context, evalNode.getRightExpr(), stack);                   // < rhs, r_nullflag
    int RHS_NULLFLAG = context.istore();
    int RHS = context.store(evalNode.getRightExpr().getValueType());           // <
    stack.pop();

    Label ifNull = new Label();
    Label afterEnd = new Label();

    context.emitNullityCheck(ifNull, LHS_NULLFLAG, RHS_NULLFLAG);

    context.load(evalNode.getLeftExpr().getValueType(), LHS);                     // < lhs
    context.load(evalNode.getRightExpr().getValueType(), RHS);            // < lhs, rhs

    context.invokeVirtual(String.class, "concat", String.class, new Class[] {String.class});
    context.pushNullFlag(true);
    context.methodvisitor.visitJumpInsn(Opcodes.GOTO, afterEnd);

    context.methodvisitor.visitLabel(ifNull);
    context.pushDummyValue(evalNode.getValueType());
    context.pushNullFlag(false);

    context.methodvisitor.visitLabel(afterEnd);

    return evalNode;
  }

  public EvalNode visitIsNull(CodeGenContext context, IsNullEval isNullEval, Stack<EvalNode> stack) {

    visit(context, isNullEval.getChild(), stack);

    Label ifNull = new Label();
    Label endIf = new Label();

    context.emitNullityCheck(ifNull);

    context.emitPop(isNullEval.getChild().getValueType());
    context.pushBooleanOfThreeValuedLogic(isNullEval.isNot() ? true : false);
    context.methodvisitor.visitJumpInsn(Opcodes.GOTO, endIf);

    context.methodvisitor.visitLabel(ifNull);
    context.emitPop(isNullEval.getChild().getValueType());
    context.pushBooleanOfThreeValuedLogic(isNullEval.isNot() ? false : true);

    emitLabel(context, endIf);
    context.methodvisitor.visitInsn(Opcodes.ICONST_1); // NOT NULL

    return isNullEval;
  }


  @Override
  public EvalNode visitConst(CodeGenContext context, ConstEval evalNode, Stack<EvalNode> stack) {
    switch (evalNode.getValueType().getType()) {
    case NULL_TYPE:
      if (!stack.isEmpty() && stack.peek() instanceof BinaryEval) {
        BinaryEval parent = (BinaryEval) stack.peek();
        if (parent.getLeftExpr() == evalNode) {
          context.pushDummyValue(parent.getRightExpr().getValueType());
        } else {
          context.pushDummyValue(parent.getLeftExpr().getValueType());
        }
      } else {
        context.push(0); // UNKNOWN
      }
      break;
    case BOOLEAN:
      context.push(evalNode.getValue().asInt4());
      break;

    case INT1:
    case INT2:
    case INT4:
      context.push(evalNode.getValue().asInt4());
      break;
    case INT8:
      context.push(evalNode.getValue().asInt8());
      break;
    case FLOAT4:
      context.push(evalNode.getValue().asFloat4());
      break;
    case FLOAT8:
      context.push(evalNode.getValue().asFloat8());
      break;
    case CHAR:
    case TEXT:
      context.push(evalNode.getValue().asChars());
      break;
    }

    context.pushNullFlag(evalNode.getValueType().getType() != TajoDataTypes.Type.NULL_TYPE);
    return evalNode;
  }

  public static ParamType [] getParamTypes(EvalNode [] arguments) {
    ParamType[] paramTypes = new ParamType[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].getType() == EvalType.CONST) {
        if (arguments[i].getValueType().getType() == TajoDataTypes.Type.NULL_TYPE) {
          paramTypes[i] = ParamType.NULL;
        } else {
          paramTypes[i] = ParamType.CONSTANT;
        }
      } else {
        paramTypes[i] = ParamType.VARIABLE;
      }
    }
    return paramTypes;
  }

  public EvalNode visitFuncCall(CodeGenContext context, GeneralFunctionEval func, Stack<EvalNode> stack) {
    int paramNum = func.getArgs().length;
    context.push(paramNum);
    context.newArray(Datum.class); // new Datum[paramNum]
    final int DATUM_ARRAY = context.astore();

    stack.push(func);
    EvalNode [] params = func.getArgs();
    for (int paramIdx = 0; paramIdx < func.getArgs().length; paramIdx++) {
      context.aload(DATUM_ARRAY);       // array ref
      context.methodvisitor.visitLdcInsn(paramIdx); // array idx
      visit(context, params[paramIdx], stack);
      context.convertToDatum(params[paramIdx].getValueType(), true);  // value
      context.methodvisitor.visitInsn(Opcodes.AASTORE);
    }
    stack.pop();

    context.push(paramNum);
    context.newArray(ParamType.class); // new Datum[paramNum]
    final int PARAM_TYPE_ARRAY = context.astore();

    ParamType [] paramTypes = getParamTypes(func.getArgs());
    for (int paramIdx = 0; paramIdx < paramTypes.length; paramIdx++) {
      context.aload(PARAM_TYPE_ARRAY);
      context.methodvisitor.visitLdcInsn(paramIdx);
      context.methodvisitor.visitFieldInsn(Opcodes.GETSTATIC, TajoGeneratorAdapter.getInternalName(ParamType.class),
          paramTypes[paramIdx].name(), TajoGeneratorAdapter.getDescription(ParamType.class));
      context.methodvisitor.visitInsn(Opcodes.AASTORE);
    }

    context.methodvisitor.visitTypeInsn(Opcodes.NEW, TajoGeneratorAdapter.getInternalName(VTuple.class));
    context.methodvisitor.visitInsn(Opcodes.DUP);
    context.aload(DATUM_ARRAY);
    context.newInstance(VTuple.class, new Class[]{Datum[].class});  // new VTuple(datum [])
    context.methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, TajoGeneratorAdapter.getInternalName(Tuple.class)); // cast to Tuple
    final int TUPLE = context.astore();

    FunctionDesc desc = func.getFuncDesc();
    try {
      context.methodvisitor.visitTypeInsn(Opcodes.NEW, TajoGeneratorAdapter.getInternalName(desc.getFuncClass()));
      int FUNC_INSTANCE = context.astore();

      context.aload(FUNC_INSTANCE);
      context.methodvisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, TajoGeneratorAdapter.getInternalName(desc.getFuncClass()),
          "<init>", "()V"); // func

      context.aload(FUNC_INSTANCE);
      context.aload(PARAM_TYPE_ARRAY);
      context.invokeVirtual(desc.getFuncClass(), "init", void.class, new Class[] {ParamType[].class});

      context.aload(FUNC_INSTANCE);
      context.aload(TUPLE);
      context.invokeVirtual(desc.getFuncClass(), "eval", Datum.class, new Class[] {Tuple.class});
    } catch (InternalException e) {
      e.printStackTrace();
    }

    context.convertToPrimitive(func.getValueType());
    return func;
  }

  public static class CodeGenContext extends TajoGeneratorAdapter {
    private Schema schema;

    public CodeGenContext(Schema schema, int access, MethodVisitor methodVisitor, String name, String desc) {
      super(access, methodVisitor, name, desc);
      this.schema = schema;
    }
  }

  private boolean canBeTransformToSwitchTable(CaseWhenEval.IfThenEval evalNode) {
    if (evalNode.getCondition().getType() == EvalType.EQUAL) {
      BinaryEval binaryEval = (BinaryEval) evalNode.getCondition();
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

  private int getSwitchIndex(CaseWhenEval.IfThenEval evalNode) {
    Preconditions.checkArgument(canBeTransformToSwitchTable(evalNode),
        "This expression cannot be used for switch table: " + evalNode);

    BinaryEval bin = (BinaryEval) evalNode.getCondition();

    if (bin.getLeftExpr().getType() == EvalType.CONST) {
      return bin.getLeftExpr().eval(null, null).asInt4();
    } else {
      return bin.getRightExpr().eval(null, null).asInt4();
    }
  }

  public static class CaseWhenSwitchGenerator implements SwitchCaseGenerator {
    final private ExprCodeGenerator generator;
    final private CodeGenContext context;
    final private Stack<EvalNode> stack;

    final NavigableMap<Integer, SwitchCase> casesMap;
    final EvalNode defaultEval;

    public CaseWhenSwitchGenerator(ExprCodeGenerator generator, CodeGenContext context, Stack<EvalNode> stack,
                                   SwitchCase[] cases, EvalNode defaultEval) {
      this.generator = generator;
      this.context = context;
      this.stack = stack;
      this.casesMap = Maps.newTreeMap();
      for (SwitchCase switchCase : cases) {
        this.casesMap.put(switchCase.key(), switchCase);
      }
      this.defaultEval = defaultEval;
    }

    @Override
    public int size() {
      return casesMap.size();
    }

    @Override
    public int min() {
      return casesMap.firstEntry().getKey();
    }

    @Override
    public int max() {
      return casesMap.lastEntry().getKey();
    }

    @Override
    public int key(int index) {
      return casesMap.get(index).key();
    }

    @Override
    public void generateCase(int key, Label end) {
      generator.visit(context, casesMap.get(key).result(), stack);
      context.gotoLabel(end);
    }

    public int [] keys() {
      int [] keys = new int[casesMap.size()];

      int idx = 0;
      for (int key : casesMap.keySet()) {
        keys[idx++] = key;
      }
      return keys;
    }

    public void generateDefault() {
      if (defaultEval != null) {
        generator.visit(context, defaultEval, stack);
      } else {
        context.pushNullOfThreeValuedLogic();
        context.pushNullFlag(false);
      }
    }
  }

  public EvalNode visitCaseWhen(CodeGenContext context, CaseWhenEval caseWhen, Stack<EvalNode> stack) {

    boolean simpleCaseWhen = true;

    for (CaseWhenEval.IfThenEval ifThen : caseWhen.getIfThenEvals()) {
      simpleCaseWhen &= canBeTransformToSwitchTable(ifThen);
    }

    stack.push(caseWhen);
    visit(context, ((BinaryEval)caseWhen.getIfThenEvals().get(0).getCondition()).getLeftExpr(), stack);
    stack.pop();

    context.pop(); // null value

    if (simpleCaseWhen) {
      int casesNum = caseWhen.getIfThenEvals().size();

      List<CaseWhenEval.IfThenEval> ifThenList = caseWhen.getIfThenEvals();

      SwitchCase [] cases = new SwitchCase[casesNum];

      for (int i = 0; i < caseWhen.getIfThenEvals().size(); i++) {
        int key = getSwitchIndex(ifThenList.get(i));
        EvalNode result = ifThenList.get(i).getResult();
        cases[i] = new SwitchCase(key, result);
      }

      CaseWhenSwitchGenerator gen = new CaseWhenSwitchGenerator(this, context, stack, cases, caseWhen.getElse());
      context.generatorAdapter.tableSwitch(gen.keys(), gen);
    } else {
    }
    return caseWhen;
  }

  public EvalNode visitIfThen(CodeGenContext context, CaseWhenEval.IfThenEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    visit(context, evalNode.getCondition(), stack);
    visit(context, evalNode.getResult(), stack);
    stack.pop();
    return evalNode;
  }

  public EvalNode visitInPredicate(CodeGenContext context, InEval evalNode, Stack<EvalNode> stack) {
    return visitBinaryEval(context, stack, evalNode);
  }
}
