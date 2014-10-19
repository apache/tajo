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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.org.objectweb.asm.Label;
import org.apache.tajo.org.objectweb.asm.MethodVisitor;
import org.apache.tajo.org.objectweb.asm.Opcodes;
import org.apache.tajo.org.objectweb.asm.Type;
import org.apache.tajo.org.objectweb.asm.commons.GeneratorAdapter;
import org.apache.tajo.org.objectweb.asm.commons.TableSwitchGenerator;

import java.util.HashMap;
import java.util.Map;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

class TajoGeneratorAdapter {

  public static final Map<EvalType, Map<TajoDataTypes.Type, Integer>> OpCodesMap = Maps.newHashMap();

  static {
    TUtil.putToNestedMap(OpCodesMap, EvalType.PLUS, INT1, Opcodes.IADD);
    TUtil.putToNestedMap(OpCodesMap, EvalType.PLUS, INT2, Opcodes.IADD);
    TUtil.putToNestedMap(OpCodesMap, EvalType.PLUS, INT4, Opcodes.IADD);
    TUtil.putToNestedMap(OpCodesMap, EvalType.PLUS, INT8, Opcodes.LADD);
    TUtil.putToNestedMap(OpCodesMap, EvalType.PLUS, FLOAT4, Opcodes.FADD);
    TUtil.putToNestedMap(OpCodesMap, EvalType.PLUS, FLOAT8, Opcodes.DADD);

    TUtil.putToNestedMap(OpCodesMap, EvalType.MINUS, INT1, Opcodes.ISUB);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MINUS, INT2, Opcodes.ISUB);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MINUS, INT4, Opcodes.ISUB);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MINUS, INT8, Opcodes.LSUB);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MINUS, FLOAT4, Opcodes.FSUB);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MINUS, FLOAT8, Opcodes.DSUB);

    TUtil.putToNestedMap(OpCodesMap, EvalType.MULTIPLY, INT1, Opcodes.IMUL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MULTIPLY, INT2, Opcodes.IMUL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MULTIPLY, INT4, Opcodes.IMUL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MULTIPLY, INT8, Opcodes.LMUL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MULTIPLY, FLOAT4, Opcodes.FMUL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MULTIPLY, FLOAT8, Opcodes.DMUL);

    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, INT1, Opcodes.IDIV);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, INT2, Opcodes.IDIV);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, INT4, Opcodes.IDIV);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, INT8, Opcodes.LDIV);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, FLOAT4, Opcodes.FDIV);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, FLOAT8, Opcodes.DDIV);

    TUtil.putToNestedMap(OpCodesMap, EvalType.MODULAR, INT1, Opcodes.IREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MODULAR, INT2, Opcodes.IREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MODULAR, INT4, Opcodes.IREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MODULAR, INT8, Opcodes.LREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MODULAR, FLOAT4, Opcodes.FREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.MODULAR, FLOAT8, Opcodes.DREM);

    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_AND, INT1, Opcodes.IAND);
    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_AND, INT2, Opcodes.IAND);
    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_AND, INT4, Opcodes.IAND);
    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_AND, INT8, Opcodes.LAND);

    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_OR, INT1, Opcodes.IOR);
    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_OR, INT2, Opcodes.IOR);
    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_OR, INT4, Opcodes.IOR);
    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_OR, INT8, Opcodes.LOR);

    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_XOR, INT1, Opcodes.IXOR);
    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_XOR, INT2, Opcodes.IXOR);
    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_XOR, INT4, Opcodes.IXOR);
    TUtil.putToNestedMap(OpCodesMap, EvalType.BIT_XOR, INT8, Opcodes.LXOR);

    TUtil.putToNestedMap(OpCodesMap, EvalType.EQUAL, INT1, Opcodes.IF_ICMPEQ);
    TUtil.putToNestedMap(OpCodesMap, EvalType.EQUAL, INT2, Opcodes.IF_ICMPEQ);
    TUtil.putToNestedMap(OpCodesMap, EvalType.EQUAL, INT4, Opcodes.IF_ICMPEQ);
    TUtil.putToNestedMap(OpCodesMap, EvalType.EQUAL, INT8, Opcodes.LCMP);
    TUtil.putToNestedMap(OpCodesMap, EvalType.EQUAL, FLOAT4, Opcodes.FCMPL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.EQUAL, FLOAT8, Opcodes.DCMPG);
    TUtil.putToNestedMap(OpCodesMap, EvalType.EQUAL, TEXT, Opcodes.IF_ACMPNE);

    TUtil.putToNestedMap(OpCodesMap, EvalType.NOT_EQUAL, INT1, Opcodes.IF_ICMPNE);
    TUtil.putToNestedMap(OpCodesMap, EvalType.NOT_EQUAL, INT2, Opcodes.IF_ICMPNE);
    TUtil.putToNestedMap(OpCodesMap, EvalType.NOT_EQUAL, INT4, Opcodes.IF_ICMPNE);
    TUtil.putToNestedMap(OpCodesMap, EvalType.NOT_EQUAL, INT8, Opcodes.LCMP);
    TUtil.putToNestedMap(OpCodesMap, EvalType.NOT_EQUAL, FLOAT4, Opcodes.FCMPL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.NOT_EQUAL, FLOAT8, Opcodes.DCMPG);
    TUtil.putToNestedMap(OpCodesMap, EvalType.NOT_EQUAL, TEXT, Opcodes.IF_ACMPNE);

    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, INT1, Opcodes.IF_ICMPLT);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, INT2, Opcodes.IF_ICMPLT);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, INT4, Opcodes.IF_ICMPLT);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, INT8, Opcodes.LCMP);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, FLOAT4, Opcodes.FCMPL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, FLOAT8, Opcodes.DCMPG);

    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, INT1, Opcodes.IF_ICMPLT);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, INT2, Opcodes.IF_ICMPLT);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, INT4, Opcodes.IF_ICMPLT);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, INT8, Opcodes.LCMP);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, FLOAT4, Opcodes.FCMPL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LTH, FLOAT8, Opcodes.DCMPG);

    TUtil.putToNestedMap(OpCodesMap, EvalType.LEQ, INT1, Opcodes.IF_ICMPLE);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LEQ, INT2, Opcodes.IF_ICMPLE);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LEQ, INT4, Opcodes.IF_ICMPLE);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LEQ, INT8, Opcodes.LCMP);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LEQ, FLOAT4, Opcodes.FCMPL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.LEQ, FLOAT8, Opcodes.DCMPG);

    TUtil.putToNestedMap(OpCodesMap, EvalType.GTH, INT1, Opcodes.IF_ICMPGT);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GTH, INT2, Opcodes.IF_ICMPGT);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GTH, INT4, Opcodes.IF_ICMPGT);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GTH, INT8, Opcodes.LCMP);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GTH, FLOAT4, Opcodes.FCMPL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GTH, FLOAT8, Opcodes.DCMPG);

    TUtil.putToNestedMap(OpCodesMap, EvalType.GEQ, INT1, Opcodes.IF_ICMPGE);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GEQ, INT2, Opcodes.IF_ICMPGE);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GEQ, INT4, Opcodes.IF_ICMPGE);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GEQ, INT8, Opcodes.LCMP);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GEQ, FLOAT4, Opcodes.FCMPL);
    TUtil.putToNestedMap(OpCodesMap, EvalType.GEQ, FLOAT8, Opcodes.DCMPG);
  }

  protected int access;
  protected MethodVisitor methodvisitor;
  protected GeneratorAdapter generatorAdapter;

  public TajoGeneratorAdapter() {}

  public TajoGeneratorAdapter(int access, MethodVisitor methodVisitor, String name, String desc) {
    this.access = access;
    this.methodvisitor = methodVisitor;
    generatorAdapter = new GeneratorAdapter(methodVisitor, access, name, desc);
  }

  public static boolean isJVMInternalInt(TajoDataTypes.DataType dataType) {
    TajoDataTypes.Type type = dataType.getType();
    return type == BOOLEAN || type == INT1 || type == INT2 || type == INT4 || type== INET4;
  }

  public static int getWordSize(TajoDataTypes.DataType type) {
    if (type.getType() == INT8 || type.getType() == FLOAT8 || type.getType() == TIMESTAMP || type.getType() == TIME) {
      return 2;
    } else {
      return 1;
    }
  }

  public void push(final boolean value) {
    methodvisitor.visitInsn(value ? Opcodes.ICONST_1 : Opcodes.ICONST_0);
  }

  public void push(final int value) {
    if (value >= -1 && value <= 5) {
      methodvisitor.visitInsn(Opcodes.ICONST_0 + value);
    } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      methodvisitor.visitIntInsn(Opcodes.BIPUSH, value);
    } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
      methodvisitor.visitIntInsn(Opcodes.SIPUSH, value);
    } else {
      methodvisitor.visitLdcInsn(new Integer(value));
    }
  }

  public void push(final long value) {
    if (value == 0L || value == 1L) {
      methodvisitor.visitInsn(Opcodes.LCONST_0 + (int) value);
    } else {
      methodvisitor.visitLdcInsn(new Long(value));
    }
  }

  public void push(final float value) {
    int bits = Float.floatToIntBits(value);
    if (bits == 0L || bits == 0x3f800000 || bits == 0x40000000) { // 0..2
      methodvisitor.visitInsn(Opcodes.FCONST_0 + (int) value);
    } else {
      methodvisitor.visitLdcInsn(new Float(value));
    }
  }

  public void push(final double value) {
    long bits = Double.doubleToLongBits(value);
    if (bits == 0L || bits == 0x3ff0000000000000L) { // +0.0d and 1.0d
      methodvisitor.visitInsn(Opcodes.DCONST_0 + (int) value);
    } else {
      methodvisitor.visitLdcInsn(new Double(value));
    }
  }

  public void push(final String value) {
    Preconditions.checkNotNull(value);
    methodvisitor.visitLdcInsn(value);
  }

  public void ifCmp(TajoDataTypes.DataType dataType, EvalType evalType, Label elseLabel) {

    if (isJVMInternalInt(dataType)) {
      switch (evalType) {
      case EQUAL:
        methodvisitor.visitJumpInsn(Opcodes.IF_ICMPNE, elseLabel);
        break;
      case NOT_EQUAL:
        methodvisitor.visitJumpInsn(Opcodes.IF_ICMPEQ, elseLabel);
        break;
      case LTH:
        methodvisitor.visitJumpInsn(Opcodes.IF_ICMPGE, elseLabel);
        break;
      case LEQ:
        methodvisitor.visitJumpInsn(Opcodes.IF_ICMPGT, elseLabel);
        break;
      case GTH:
        methodvisitor.visitJumpInsn(Opcodes.IF_ICMPLE, elseLabel);
        break;
      case GEQ:
        methodvisitor.visitJumpInsn(Opcodes.IF_ICMPLT, elseLabel);
        break;
      default:
        throw new CompilationError("Unknown comparison operator: " + evalType.name());
      }
    } else {

      if (dataType.getType() == TEXT) {
        invokeVirtual(String.class, "compareTo", int.class, new Class[]{String.class});
      } else {
        int opCode = TajoGeneratorAdapter.getOpCode(evalType, dataType);
        methodvisitor.visitInsn(opCode);
      }

      switch (evalType) {
      case EQUAL:
        methodvisitor.visitJumpInsn(Opcodes.IFNE, elseLabel);
        break;
      case NOT_EQUAL:
        methodvisitor.visitJumpInsn(Opcodes.IFEQ, elseLabel);
        break;
      case LTH:
        methodvisitor.visitJumpInsn(Opcodes.IFGE, elseLabel);
        break;
      case LEQ:
        methodvisitor.visitJumpInsn(Opcodes.IFGT, elseLabel);
        break;
      case GTH:
        methodvisitor.visitJumpInsn(Opcodes.IFLE, elseLabel);
        break;
      case GEQ:
        methodvisitor.visitJumpInsn(Opcodes.IFLT, elseLabel);
        break;
      default:
        throw new CompilationError("Unknown comparison operator: " + evalType.name());
      }
    }
  }

  public void load(TajoDataTypes.DataType dataType, int idx) {
    switch (dataType.getType()) {
    case NULL_TYPE:
    case BOOLEAN:
    case CHAR:
    case INT1:
    case INT2:
    case INT4:
    case INET4:
      methodvisitor.visitVarInsn(Opcodes.ILOAD, idx);
      break;
    case INT8:
      methodvisitor.visitVarInsn(Opcodes.LLOAD, idx);
      break;
    case FLOAT4:
      methodvisitor.visitVarInsn(Opcodes.FLOAD, idx);
      break;
    case FLOAT8:
      methodvisitor.visitVarInsn(Opcodes.DLOAD, idx);
      break;
    case TEXT:
    case INTERVAL:
    case PROTOBUF:
      methodvisitor.visitVarInsn(Opcodes.ALOAD, idx);
      break;
    default:
      throw new CompilationError("Unknown data type: " + dataType.getType().name());
    }
  }

  public static String getDescription(Class clazz) {
    if (clazz == null) {
      return "";
    } else if (clazz == void.class) {
      return "V";
    } else if (clazz == boolean.class) {
      return "Z";
    } else if (clazz == char.class) {
      return "C";
    } else if (clazz == byte.class) {
      return "B";
    } else if (clazz == short.class) {
      return "S";
    } else if (clazz == int.class) {
      return "I";
    } else if (clazz == long.class) {
      return "J";
    } else if (clazz == float.class) {
      return "F";
    } else if (clazz == double.class) {
      return "D";
    } else if (clazz.isArray()) {
      return "[" + getDescription(clazz.getComponentType());
    } else {
      return "L" + getInternalName(clazz) + ";";
    }
  }

  public static String getMethodDescription(Class returnType, Class [] argumentTypes) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    if (argumentTypes != null) {
      for (Class argType : argumentTypes) {
        builder.append(getDescription(argType));
      }
    }
    builder.append(")");

    builder.append(getDescription(returnType));
    return builder.toString();
  }

  public Label newLabel() {
    return new Label();
  }

  public void markLabel(Label label) {
    methodvisitor.visitLabel(label);
  }

  public void gotoLabel(Label label) {
    methodvisitor.visitJumpInsn(Opcodes.GOTO, label);
  }

  public void pushBooleanOfThreeValuedLogic(boolean value) {
    push(value ? 1 : 2); // TRUE or FALSE
  }

  public void pushNullOfThreeValuedLogic() {
    push(0); // NULL of three valued logic
  }

  public void pushNullFlag(boolean trueIfNotNull) {
    push(trueIfNotNull ? true : false);
  }

  public void emitNullityCheck(Label ifNull) {
    methodvisitor.visitJumpInsn(Opcodes.IFEQ, ifNull);
  }

  /**
   * If at least one of all local variables corresponding to <code>varIds</code> is null, jump the <code>label</code>.
   *
   * @param ifNull The label to jump
   * @param varIds A list of variable Ids.
   */
  public void emitNullityCheck(Label ifNull, int ... varIds) {
    // TODO - ANDing can be reduced if we interleave IAND into a sequence of ILOAD instructions.
    for (int varId : varIds) {
      methodvisitor.visitVarInsn(Opcodes.ILOAD, varId);
    }
    if (varIds.length > 1) {
      for (int i = 0; i < varIds.length - 1; i++) {
        methodvisitor.visitInsn(Opcodes.IAND);
      }
    }
    emitNullityCheck(ifNull);
  }

  public void pushDummyValue(TajoDataTypes.DataType type) {
    if (type.getType() == NULL_TYPE) {
      pushNullOfThreeValuedLogic();
    } else if (isJVMInternalInt(type) || type.getType() == DATE) {
      push(0);
    } else if (type.getType() == TajoDataTypes.Type.INT8 || type.getType() == TIMESTAMP || type.getType() == TIME) {
      push(0L);
    } else if (type.getType() == TajoDataTypes.Type.FLOAT8) {
      push(0.0d);
    } else if (type.getType() == TajoDataTypes.Type.FLOAT4) {
      push(0.0f);
    } else if (type.getType() == TajoDataTypes.Type.CHAR || type.getType() == TajoDataTypes.Type.TEXT) {
      push("");
    } else if (type.getType() == INTERVAL || type.getType() == PROTOBUF) {
      invokeStatic(NullDatum.class, "get", NullDatum.class, new Class[]{});
    } else {
      assert false;
    }
  }

  public void newInstance(Class owner, Class [] paramTypes) {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, getInternalName(owner), "<init>",
        getMethodDescription(void.class, paramTypes));
  }

  public void invokeSpecial(Class owner, String methodName, Class returnType, Class [] paramTypes) {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, getInternalName(owner), methodName,
        getMethodDescription(returnType, paramTypes));
  }

  public void invokeStatic(Class owner, String methodName, Class returnType, Class [] paramTypes) {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, getInternalName(owner), methodName,
        getMethodDescription(returnType, paramTypes));
  }

  public void invokeVirtual(Class owner, String methodName, Class returnType, Class [] paramTypes) {
    methodvisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(owner), methodName,
        getMethodDescription(returnType, paramTypes));
  }

  public void invokeInterface(Class owner, String methodName, Class returnType, Class [] paramTypes) {
    methodvisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(owner), methodName,
        getMethodDescription(returnType, paramTypes));
  }

  public static boolean isPrimitiveOpCode(EvalType evalType, TajoDataTypes.DataType returnType) {
    return TUtil.containsInNestedMap(OpCodesMap, evalType, returnType.getType());
  }

  public static int getOpCode(EvalType evalType, TajoDataTypes.DataType returnType) {
    if (!isPrimitiveOpCode(evalType, returnType)) {
      throw new CompilationError("No Such OpCode for " + evalType + " returning " + returnType.getType().name());
    }
    return TUtil.getFromNestedMap(OpCodesMap, evalType, returnType.getType());
  }

  public void castInsn(TajoDataTypes.DataType srcType, TajoDataTypes.DataType targetType) {
    TajoDataTypes.Type srcRawType = srcType.getType();
    TajoDataTypes.Type targetRawType = targetType.getType();
    switch(srcRawType) {
    case BOOLEAN:
    case CHAR: {
      if (srcType.hasLength() && srcType.getLength() == 1) {
        switch (targetType.getType()) {
        case CHAR:
        case INT1:
        case INT2:
        case INT4: break;
        case INT8:   methodvisitor.visitInsn(Opcodes.I2L); break;
        case FLOAT4: methodvisitor.visitInsn(Opcodes.I2F); break;
        case FLOAT8: methodvisitor.visitInsn(Opcodes.I2D); break;
        case TEXT:   emitStringValueOfChar(); break;
        default:
          throw new InvalidCastException(srcType, targetType);
        }
      } else {
        switch (targetRawType) {
        case CHAR:
        case INT1:
        case INT2:
        case INT4: emitParseInt4(); break;
        case INT8: emitParseInt8(); break;
        case FLOAT4: emitParseFloat4(); break;
        case FLOAT8: emitParseFloat8(); break;
        case TEXT: break;
        default: throw new InvalidCastException(srcType, targetType);
        }
      }
      break;
    }
    case INT1:
    case INT2:
    case INT4:
      switch (targetType.getType()) {
      case CHAR:
      case INT1: methodvisitor.visitInsn(Opcodes.I2C); break;
      case INT2: methodvisitor.visitInsn(Opcodes.I2S); break;
      case INT4: return;
      case INT8: methodvisitor.visitInsn(Opcodes.I2L); break;
      case FLOAT4: methodvisitor.visitInsn(Opcodes.I2F); break;
      case FLOAT8: methodvisitor.visitInsn(Opcodes.I2D); break;
      case TEXT: emitStringValueOfInt4(); break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case INT8:
      switch (targetRawType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: methodvisitor.visitInsn(Opcodes.L2I); break;
      case INT8: return;
      case FLOAT4: methodvisitor.visitInsn(Opcodes.L2F); break;
      case FLOAT8: methodvisitor.visitInsn(Opcodes.L2D); break;
      case TEXT: emitStringValueOfInt8(); break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case FLOAT4:
      switch (targetRawType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: methodvisitor.visitInsn(Opcodes.F2I); break;
      case INT8: methodvisitor.visitInsn(Opcodes.F2L); break;
      case FLOAT4: return;
      case FLOAT8: methodvisitor.visitInsn(Opcodes.F2D); break;
      case TEXT: emitStringValueOfFloat4(); break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case FLOAT8:
      switch (targetRawType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: methodvisitor.visitInsn(Opcodes.D2I); break;
      case INT8: methodvisitor.visitInsn(Opcodes.D2L); break;
      case FLOAT4: methodvisitor.visitInsn(Opcodes.D2F); break;
      case FLOAT8: return;
      case TEXT: emitStringValueOfFloat8(); break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case TEXT:
      switch (targetRawType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: emitParseInt4(); break;
      case INT8: emitParseInt8(); break;
      case FLOAT4: emitParseFloat4(); break;
      case FLOAT8: emitParseFloat8(); break;
      case TEXT: break;
      case TIMESTAMP: {
        methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(DateTimeUtil.class),
            "toJulianTimestampWithTZ", "(L" + Type.getInternalName(String.class) + ";)J");
        break;
      }
      case DATE: {
        methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(DateTimeUtil.class),
            "toJulianDate", "(L" + Type.getInternalName(String.class) + ";)I");
        break;
      }
      case TIME: {
        methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(DateTimeUtil.class),
            "toJulianTime", "(L" + Type.getInternalName(String.class) + ";)J");
        break;
      }
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    default: throw new InvalidCastException(srcType, targetType);
    }
  }

  public static String getInternalName(String className) {
    return className.replace('.', '/');
  }

  public static String getInternalName(Class clazz) {
    return clazz.getName().replace('.', '/');
  }

  public void convertToPrimitive(TajoDataTypes.DataType type) {

    Label ifNull = new Label();
    Label afterAll = new Label();

    // datum
    int datum = astore();

    aload(datum);
    invokeVirtual(Datum.class, "isNotNull", boolean.class, new Class [] {});
    methodvisitor.visitJumpInsn(Opcodes.IFEQ, ifNull);  // datum

    aload(datum);
    switch (type.getType()) {
    case BOOLEAN:
    case INT1:
    case INT2:
      invokeVirtual(Datum.class, "asInt2", short.class, new Class[] {});
      break;
    case INT4:
    case DATE:
      invokeVirtual(Datum.class, "asInt4", int.class, new Class[] {});
      break;
    case INT8:
    case TIMESTAMP:
    case TIME:
      invokeVirtual(Datum.class, "asInt8", long.class, new Class[] {});
      break;
    case FLOAT4:
      invokeVirtual(Datum.class, "asFloat4", float.class, new Class[] {});
      break;
    case FLOAT8:
      invokeVirtual(Datum.class, "asFloat8", double.class, new Class[] {});
      break;
    case CHAR:
    case TEXT:
      invokeVirtual(Datum.class, "asChars", String.class, new Class[]{});
      break;
    default:
      throw new UnsupportedException("Unsupported type: " + type);
    }

    pushNullFlag(true);
    gotoLabel(afterAll);

    methodvisitor.visitLabel(ifNull);
    pushDummyValue(type);
    pushNullFlag(false);

    methodvisitor.visitLabel(afterAll);
  }

  public void convertToDatum(TajoDataTypes.DataType type, boolean castToDatum) {
    String convertMethod;
    Class returnType;
    Class [] paramTypes;
    switch (type.getType()) {
    case NULL_TYPE:
      pop();      // pop null flag
      pop(type);  // pop null datum
      invokeStatic(NullDatum.class, "get", NullDatum.class, new Class[] {});
      if (castToDatum) {
        methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, getInternalName(Datum.class));
      }
      return;

    case BOOLEAN:
      convertMethod = "createBool";
      returnType = Datum.class;
      paramTypes = new Class[] {int.class};
      break;
    case CHAR:
      convertMethod = "createChar";
      returnType = CharDatum.class;
      paramTypes = new Class[] {String.class};
      break;
    case INT1:
    case INT2:
      convertMethod = "createInt2";
      returnType = Int2Datum.class;
      paramTypes = new Class[] {short.class};
      break;
    case INT4:
      convertMethod = "createInt4";
      returnType = Int4Datum.class;
      paramTypes = new Class[] {int.class};
      break;
    case INT8:
      convertMethod = "createInt8";
      returnType = Int8Datum.class;
      paramTypes = new Class[] {long.class};
      break;
    case FLOAT4:
      convertMethod = "createFloat4";
      returnType = Float4Datum.class;
      paramTypes = new Class[] {float.class};
      break;
    case FLOAT8:
      convertMethod = "createFloat8";
      returnType = Float8Datum.class;
      paramTypes = new Class[] {double.class};
      break;
    case TEXT:
      convertMethod = "createText";
      returnType = TextDatum.class;
      paramTypes = new Class[] {String.class};
      break;
    case TIMESTAMP:
      convertMethod = "createTimestamp";
      returnType = TimestampDatum.class;
      paramTypes = new Class[] {long.class};
      break;
    case DATE:
      convertMethod = "createDate";
      returnType = DateDatum.class;
      paramTypes = new Class[] {int.class};
      break;
    case TIME:
      convertMethod = "createTime";
      returnType = TimeDatum.class;
      paramTypes = new Class[] {long.class};
      break;
    case INTERVAL:
    case PROTOBUF:
      convertMethod = null;
      returnType = null;
      paramTypes = null;
      break;
    case INET4:
      convertMethod = "createInet4";
      returnType = Inet4Datum.class;
      paramTypes = new Class[] {int.class};
      break;
    default:
      throw new RuntimeException("Unsupported type: " + type.getType().name());
    }

    Label ifNull = new Label();
    Label afterAll = new Label();

    emitNullityCheck(ifNull);
    if (convertMethod != null) {
      invokeStatic(DatumFactory.class, convertMethod, returnType, paramTypes);
    }
    methodvisitor.visitJumpInsn(Opcodes.GOTO, afterAll);

    methodvisitor.visitLabel(ifNull);
    pop(type);
    invokeStatic(NullDatum.class, "get", NullDatum.class, null);

    methodvisitor.visitLabel(afterAll);
    if (castToDatum) {
      methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, TajoGeneratorAdapter.getInternalName(Datum.class));
    }
  }

  public void pop(TajoDataTypes.DataType type) {
    if (getWordSize(type) == 2) {
      methodvisitor.visitInsn(Opcodes.POP2);
    } else {
      methodvisitor.visitInsn(Opcodes.POP);
    }
  }

  public void emitStringValueOfChar() {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(C)L" + Type.getInternalName(String.class) + ";");
  }

  public void emitStringValueOfInt4() {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(I)L" + Type.getInternalName(String.class) + ";");
  }

  public void emitStringValueOfInt8() {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(J)L" + Type.getInternalName(String.class) + ";");
  }

  public void emitStringValueOfFloat4() {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(F)L" + Type.getInternalName(String.class) + ";");
  }

  public void emitStringValueOfFloat8() {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(D)L" + Type.getInternalName(String.class) + ";");
  }

  public void emitParseInt4() {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Integer.class),
        "parseInt", "(L" + Type.getInternalName(String.class) + ";)I");
  }

  public void emitParseInt8() {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Long.class),
        "parseLong", "(L" + Type.getInternalName(String.class) + ";)J");
  }

  public void emitParseFloat4() {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Float.class),
        "parseFloat", "(L" + Type.getInternalName(String.class) + ";)F");
  }

  public void emitParseFloat8() {
    methodvisitor.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Double.class),
        "parseDouble", "(L" + Type.getInternalName(String.class) + ";)D");
  }

  public void newArray(final Class clazz) {
    int typeCode;
    if (clazz == boolean.class) {
      typeCode = Opcodes.T_BOOLEAN;
    } else if (clazz == char.class) {
      typeCode = Opcodes.T_CHAR;
    } else if (clazz == byte.class) {
      typeCode = Opcodes.T_BYTE;
    } else if (clazz == short.class) {
      typeCode = Opcodes.T_SHORT;
    } else if (clazz == int.class) {
      typeCode = Opcodes.T_INT;
    } else if (clazz == long.class) {
      typeCode = Opcodes.T_LONG;
    } else if (clazz == float.class) {
      typeCode = Opcodes.T_FLOAT;
    } else if (clazz == double.class) {
      typeCode = Opcodes.T_DOUBLE;
    } else {
      methodvisitor.visitTypeInsn(Opcodes.ANEWARRAY, getInternalName(clazz));
      return;
    }

    methodvisitor.visitIntInsn(Opcodes.NEWARRAY, typeCode);
  }

  private int nextVarId = 3;

  private Map<String, Integer> localVariablesMap = new HashMap<String, Integer>();

  public void astore(String name) {
    if (localVariablesMap.containsKey(name)) {
      int varId = localVariablesMap.get(name);
      methodvisitor.visitVarInsn(Opcodes.ASTORE, varId);
    } else {
      int varId = nextVarId++;
      methodvisitor.visitVarInsn(Opcodes.ASTORE, varId);
      localVariablesMap.put(name, varId);
    }
  }

  public int astore() {
    int varId = getCurVarIdAndIncrease();
    methodvisitor.visitVarInsn(Opcodes.ASTORE, varId);
    return varId;
  }

  public void astore(int varId) {
    methodvisitor.visitVarInsn(Opcodes.ASTORE, varId);
  }

  public void aload(String name) {
    if (localVariablesMap.containsKey(name)) {
      int varId = localVariablesMap.get(name);
      methodvisitor.visitVarInsn(Opcodes.ALOAD, varId);
    } else {
      throw new RuntimeException("No such variable name: " + name);
    }
  }

  public void aload(int varId) {
    methodvisitor.visitVarInsn(Opcodes.ALOAD, varId);
  }

  public void dup() {
    methodvisitor.visitInsn(Opcodes.DUP);
  }

  public void pop() {
    methodvisitor.visitInsn(Opcodes.POP);
  }

  public void pop2() {
    methodvisitor.visitInsn(Opcodes.POP2);
  }

  public int istore() {
    int varId = getCurVarIdAndIncrease();
    return istore(varId);
  }

  public int istore(int varId) {
    methodvisitor.visitVarInsn(Opcodes.ISTORE, varId);
    return varId;
  }

  public void iload(int varId) {
    methodvisitor.visitVarInsn(Opcodes.ILOAD, varId);
  }

  private int getCurVarIdAndIncrease() {
    int varId = nextVarId++;
    return varId;
  }

  private int getCurVarIdAndIncrease(TajoDataTypes.DataType type) {
    int varId = nextVarId;
    nextVarId += getWordSize(type);
    return varId;
  }

  public int store(TajoDataTypes.DataType type) {
    int varId = nextVarId;
    nextVarId += TajoGeneratorAdapter.getWordSize(type);

    switch (type.getType()) {
    case NULL_TYPE:
    case BOOLEAN:
    case CHAR:
    case INT1:
    case INT2:
    case INT4:
    case INET4:
      methodvisitor.visitVarInsn(Opcodes.ISTORE, varId);
      break;
    case TIME:
    case TIMESTAMP:
    case INT8:
      methodvisitor.visitVarInsn(Opcodes.LSTORE, varId);
      break;
    case FLOAT4:
      methodvisitor.visitVarInsn(Opcodes.FSTORE, varId);
      break;
    case FLOAT8:
      methodvisitor.visitVarInsn(Opcodes.DSTORE, varId);
      break;
    case INTERVAL:
    case TEXT:
      methodvisitor.visitVarInsn(Opcodes.ASTORE, varId);
      break;
    default:
      throw new CompilationError("Unknown data type: " + type.getType().name());
    }

    return varId;
  }

  public void emitBoxing(EvalCodeGenContext context, TajoDataTypes.DataType dataType) {
    switch (dataType.getType()) {
    case CHAR:
    case TEXT:

    case INT2:
      context.invokeStatic(Short.class, "valueOf", Short.class, new Class[]{short.class});
      break;
    case INT4:
      context.invokeStatic(Integer.class, "valueOf", Integer.class, new Class[]{int.class});
      break;
    case INT8:
      context.invokeStatic(Long.class, "valueOf", Long.class, new Class[]{long.class});
      break;
    case FLOAT4:
      context.invokeStatic(Float.class, "valueOf", Float.class, new Class[]{float.class});
      break;
    case FLOAT8:
      context.invokeStatic(Double.class, "valueOf", Double.class, new Class[]{double.class});
      break;

    default:
      throw new RuntimeException(dataType.getType().name() + " is not supported yet");
    }
  }

  public void emitUnboxing(EvalCodeGenContext context, TajoDataTypes.DataType dataType) {
    switch (dataType.getType()) {
    case CHAR:
    case TEXT:

    case INT2:
      context.invokeVirtual(Short.class, "shortValue", short.class, new Class[]{});
      break;
    case INT4:
      context.invokeVirtual(Integer.class, "intValue", int.class, new Class[]{});
      break;
    case INT8:
      context.invokeVirtual(Long.class, "longValue", long.class, new Class[]{});
      break;
    case FLOAT4:
      context.invokeVirtual(Float.class, "floatValue", float.class, new Class[]{});
      break;
    case FLOAT8:
      context.invokeVirtual(Double.class, "doubleValue", double.class, new Class[]{});
      break;

    default:
      throw new RuntimeException(dataType.getType().name() + " is not supported yet");
    }
  }

  public static interface SwitchCaseGenerator extends TableSwitchGenerator {
    int size();
    int min();
    int max();
    int key(int index);
    void generateCase(int index, Label end);
    void generateDefault();
  }

  public static class SwitchCase implements Comparable<SwitchCase> {
    private final int index;
    private final EvalNode thanResult;

    public SwitchCase(int index, EvalNode thanResult) {
      this.index = index;
      this.thanResult = thanResult;
    }

    public int key() {
      return index;
    }

    public EvalNode result() {
      return thanResult;
    }

    @Override
    public int compareTo(SwitchCase o) {
      return index - o.index;
    }
  }
}
