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
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.PrintStream;
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
    if (EvalType.isLogicalOperator(binaryEval)) {
      return visitAndOrEval(context, binaryEval, stack);
    } else if (EvalType.isArithmeticOperator(binaryEval)) {
      return visitArithmeticEval(context, binaryEval, stack);
    } else if (EvalType.isComparisonOperator(binaryEval)) {
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

      visit(context, unary.getChild(), stack);
      context.method.visitVarInsn(Opcodes.ISTORE, 9);
      context.method.visitVarInsn(Opcodes.ISTORE, 10);

      Label ifNull = new Label();
      Label endIf = new Label();

      context.emitNullityCheck(ifNull, 9);

      context.method.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(ExprCodeGenerator.class),
          "NOT_LOGIC", "[B");
      context.method.visitVarInsn(Opcodes.ILOAD, 10);
      context.method.visitInsn(Opcodes.BALOAD);
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
      case INT4: context.method.visitInsn(Opcodes.INEG); break;
      case INT8: context.method.visitInsn(Opcodes.LNEG); break;
      case FLOAT4: context.method.visitInsn(Opcodes.FNEG); break;
      case FLOAT8: context.method.visitInsn(Opcodes.DNEG); break;
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
    context.method.visitVarInsn(Opcodes.ISTORE, predNullVarId);      // < predicand (store nullflag to 3)
    int beginNullVarId = emitStore(context, predicand, predVarId);    // <

    visit(context, begin, stack);                                    // < begin, left_nullflag
    context.method.visitVarInsn(Opcodes.ISTORE, beginNullVarId);  // < begin, store left_nullflag to x
    int beginVarId = beginNullVarId + 1;
    int endNullVarId = emitStore(context, begin, beginVarId);

    visit(context, end, stack);                                         // < end, right_nullflag
    context.method.visitVarInsn(Opcodes.ISTORE, endNullVarId);      // < end, store right_nullflag
    int endVarId = endNullVarId + 1;
    emitStore(context, end, endVarId);                                // <

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
      context.method.visitInsn(Opcodes.IOR);
      context.method.visitJumpInsn(Opcodes.IFEQ, ifNotMatched);
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
    context.method.visitJumpInsn(Opcodes.GOTO, label);
  }

  private void emitLabel(CodeGenContext context, Label label) {
    context.method.visitLabel(label);
  }

  public EvalNode generate(Schema schema, EvalNode expr) throws NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, InstantiationException, PlanningException {

    ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    classWriter.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test3", null, GeneratorAdapter.getInternalName(EvalNode
        .class), null);
    classWriter.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
        null, null).visitEnd();

    // constructor method
    MethodVisitor methodVisitor = classWriter.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, GeneratorAdapter.getInternalName(EvalNode.class), "<init>",
        "()V");
    methodVisitor.visitInsn(Opcodes.RETURN);
    methodVisitor.visitMaxs(1, 1);
    methodVisitor.visitEnd();

    // method
    MethodVisitor evalMethod = classWriter.visitMethod(Opcodes.ACC_PUBLIC, "eval",
        GeneratorAdapter.getMethodDescription(Datum.class, new Class [] {Schema.class, Tuple.class}), null, null);
    evalMethod.visitCode();
    evalMethod.visitVarInsn(Opcodes.ALOAD, 0);

    CodeGenContext context = new CodeGenContext(evalMethod, schema);

    visit(context, expr, new Stack<EvalNode>());

    context.convertToDatum(expr.getValueType(), true);
    context.method.visitInsn(Opcodes.ARETURN);
    context.method.visitMaxs(0, 0);
    context.method.visitEnd();
    classWriter.visitEnd();

    TestExprCodeGenerator.MyClassLoader myClassLoader = new TestExprCodeGenerator.MyClassLoader();
    Class aClass = myClassLoader.defineClass("org.Test3", classWriter.toByteArray());
    Constructor constructor = aClass.getConstructor();
    EvalNode r = (EvalNode) constructor.newInstance();
    return r;
  }

  private void printOut(CodeGenContext context, String message) {
    context.method.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
    context.push(message);
    context.invokeVirtual(PrintStream.class, "println", void.class, new Class [] {String.class});
  }

  public EvalNode visitCast(CodeGenContext context, Stack<EvalNode> stack, CastEval cast) {
    TajoDataTypes.DataType  srcType = cast.getOperand().getValueType();
    TajoDataTypes.DataType targetType = cast.getValueType();

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

      context.method.visitVarInsn(Opcodes.ALOAD, 2);
      context.push(idx);
      context.invokeInterface(Tuple.class, "isNull", boolean.class, new Class [] {int.class});

      context.push(true);

      Label ifNull = new Label();
      Label afterAll = new Label();
      context.method.visitJumpInsn(Opcodes.IF_ICMPEQ, ifNull);

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

      context.method.visitVarInsn(Opcodes.ALOAD, 2);
      context.push(idx);
      context.invokeInterface(Tuple.class, methodName, returnType, paramTypes);

      context.pushNullFlag(true); // not null
      context.method.visitJumpInsn(Opcodes.GOTO, afterAll);

      context.method.visitLabel(ifNull);
      context.pushDummyValue(field.getValueType());
      context.pushNullFlag(false);

      context.method.visitLabel(afterAll);
    }
    return field;
  }

  public EvalNode visitAndOrEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {

    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);
    context.method.visitVarInsn(Opcodes.ISTORE, 3);
    context.method.visitVarInsn(Opcodes.ISTORE, 4);

    visit(context, evalNode.getRightExpr(), stack);
    context.method.visitVarInsn(Opcodes.ISTORE, 5);
    context.method.visitVarInsn(Opcodes.ISTORE, 6);
    stack.pop();

    Label ifNullCommon = new Label();
    Label afterEnd = new Label();

    context.emitNullityCheck(ifNullCommon, 3, 5);

    if (evalNode.getType() == EvalType.AND) {
      context.method.visitFieldInsn(Opcodes.GETSTATIC,
          org.objectweb.asm.Type.getInternalName(ExprCodeGenerator.class), "AND_LOGIC", "[[B");
    } else if (evalNode.getType() == EvalType.OR) {
      context.method.visitFieldInsn(Opcodes.GETSTATIC,
          org.objectweb.asm.Type.getInternalName(ExprCodeGenerator.class), "OR_LOGIC", "[[B");
    } else {
      throw new CodeGenException("visitAndOrEval() cannot generate the code at " + evalNode);
    }
    context.load(evalNode.getLeftExpr().getValueType(), 4);
    context.method.visitInsn(Opcodes.AALOAD);
    context.load(evalNode.getRightExpr().getValueType(), 6);
    context.method.visitInsn(Opcodes.BALOAD);
    context.method.visitInsn(Opcodes.ICONST_1);
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNullCommon);
    context.pushNullOfThreeValuedLogic();
    context.pushNullFlag(false);

    emitLabel(context, afterEnd);

    return evalNode;
  }

  public static int emitStore(CodeGenContext context, EvalNode evalNode, int idx) {
    switch (evalNode.getValueType().getType()) {
    case NULL_TYPE:
    case BOOLEAN:
    case CHAR:
    case INT1:
    case INT2:
    case INT4:
      context.method.visitVarInsn(Opcodes.ISTORE, idx);
      break;
    case INT8: context.method.visitVarInsn(Opcodes.LSTORE, idx); break;
    case FLOAT4: context.method.visitVarInsn(Opcodes.FSTORE, idx); break;
    case FLOAT8: context.method.visitVarInsn(Opcodes.DSTORE, idx); break;
    default: context.method.visitVarInsn(Opcodes.ASTORE, idx); break;
    }

    return idx + GeneratorAdapter.getWordSize(evalNode.getValueType());
  }

  public EvalNode visitArithmeticEval(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);          // < left_child, push nullflag
    context.method.visitVarInsn(Opcodes.ISTORE, 3);     // < left_child
    int rNullVarId = emitStore(context, evalNode.getLeftExpr(), 4);

    visit(context, evalNode.getRightExpr(), stack);         // < left_child, right_child, nullflag
    context.method.visitVarInsn(Opcodes.ISTORE, rNullVarId);     // < left_child, right_child
    int rValVarId = rNullVarId + 1;
    emitStore(context, evalNode.getRightExpr(), rValVarId);
    stack.pop();

    Label ifNullCommon = new Label();
    Label afterEnd = new Label();

    context.emitNullityCheck(ifNullCommon, 3, rNullVarId);

    context.load(evalNode.getLeftExpr().getValueType(), 4);
    context.load(evalNode.getRightExpr().getValueType(), rValVarId);

    int opCode = GeneratorAdapter.getOpCode(evalNode.getType(), evalNode.getValueType());
    context.method.visitInsn(opCode);
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

    stack.push(evalNode);

    visit(context, evalNode.getLeftExpr(), stack);                    // < lhs, l_null
    context.method.visitVarInsn(Opcodes.ISTORE, 3);               // < lhs
    int rNullVarId = emitStore(context, evalNode.getLeftExpr(), 4);   // <

    visit(context, evalNode.getRightExpr(), stack);                   // < rhs, r_nullflag
    context.method.visitVarInsn(Opcodes.ISTORE, rNullVarId);      // < rhs
    int rValVarId = rNullVarId + 1;
    emitStore(context, evalNode.getRightExpr(), rValVarId);           // <
    stack.pop();

    Label ifNull = new Label();
    Label ifNotMatched = new Label();
    Label afterEnd = new Label();

    context.emitNullityCheck(ifNull, 3, rNullVarId);

    context.load(evalNode.getLeftExpr().getValueType(), 4);                     // < lhs
    context.load(evalNode.getRightExpr().getValueType(), rValVarId);            // < lhs, rhs

    context.ifCmp(evalNode.getLeftExpr().getValueType(), evalNode.getType(), ifNotMatched);

    context.pushBooleanOfThreeValuedLogic(true);
    context.pushNullFlag(true);
    context.method.visitJumpInsn(Opcodes.GOTO, afterEnd);

    context.method.visitLabel(ifNotMatched);
    context.pushBooleanOfThreeValuedLogic(false);
    context.pushNullFlag(true);
    context.method.visitJumpInsn(Opcodes.GOTO, afterEnd);

    context.method.visitLabel(ifNull);
    context.pushNullOfThreeValuedLogic();
    context.pushNullFlag(false);

    context.method.visitLabel(afterEnd);

    return evalNode;
  }

  public EvalNode visitIsNull(CodeGenContext context, IsNullEval isNullEval, Stack<EvalNode> stack) {

    visit(context, isNullEval.getChild(), stack);

    Label ifNull = new Label();
    Label endIf = new Label();

    context.emitNullityCheck(ifNull);

    context.emitPop(isNullEval.getChild().getValueType());
    context.pushBooleanOfThreeValuedLogic(isNullEval.isNot() ? true : false);
    context.method.visitJumpInsn(Opcodes.GOTO, endIf);

    context.method.visitLabel(ifNull);
    context.emitPop(isNullEval.getChild().getValueType());
    context.pushBooleanOfThreeValuedLogic(isNullEval.isNot() ? false : true);

    emitLabel(context, endIf);
    context.method.visitInsn(Opcodes.ICONST_1); // NOT NULL

    return isNullEval;
  }


  @Override
  public EvalNode visitConst(CodeGenContext context, ConstEval evalNode, Stack<EvalNode> stack) {
    switch (evalNode.getValueType().getType()) {
    case NULL_TYPE:
      //context.push(0); // UNKNOWN
      context.push(evalNode.getValue().asInt4());
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

  public EvalNode visitFuncCall(CodeGenContext context, GeneralFunctionEval func, Stack<EvalNode> stack) {


    int paramNum = func.getArgs().length;
    context.push(paramNum);
    context.newArray(Datum.class); // new Datum[paramNum]
    context.astore(3);

    stack.push(func);
    EvalNode [] params = func.getArgs();
    for (int paramIdx = 0; paramIdx < func.getArgs().length; paramIdx++) {
      context.aload(3);       // array ref
      context.method.visitLdcInsn(paramIdx); // array idx
      visit(context, params[paramIdx], stack);
      context.convertToDatum(params[paramIdx].getValueType(), true);  // value
      context.method.visitInsn(Opcodes.AASTORE);
    }
    stack.pop();

    context.method.visitTypeInsn(Opcodes.NEW, GeneratorAdapter.getInternalName(VTuple.class));
    context.method.visitInsn(Opcodes.DUP);
    context.aload(3);
    context.newInstance(VTuple.class, new Class[]{Datum[].class});
    context.method.visitTypeInsn(Opcodes.CHECKCAST, GeneratorAdapter.getInternalName(Tuple.class));
    context.astore(5);

    FunctionDesc desc = func.getFuncDesc();
    try {

      context.method.visitTypeInsn(Opcodes.NEW, GeneratorAdapter.getInternalName(desc.getFuncClass()));
      context.method.visitInsn(Opcodes.DUP);
      context.method.visitMethodInsn(Opcodes.INVOKESPECIAL, GeneratorAdapter.getInternalName(desc.getFuncClass()),
          "<init>", "()V");
      context.aload(5);
      context.invokeVirtual(desc.getFuncClass(), "eval", Datum.class, new Class[] {Tuple.class});
    } catch (InternalException e) {
      e.printStackTrace();
    }

    context.convertToJavaType(func.getValueType());
    context.pushNullFlag(true);
    return func;
  }

  public static class CodeGenContext extends GeneratorAdapter {
    private Schema schema;

    public CodeGenContext(MethodVisitor methodVisitor, Schema schema) {
      super(methodVisitor);
      this.schema = schema;
    }
  }
}
