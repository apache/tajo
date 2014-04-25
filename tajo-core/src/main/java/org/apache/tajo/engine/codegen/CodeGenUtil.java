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

import com.google.common.collect.Maps;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.util.TUtil;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Map;

import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.apache.tajo.common.TajoDataTypes.Type.INT4;
import static org.apache.tajo.common.TajoDataTypes.Type.INT8;

public class CodeGenUtil {

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
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, FLOAT8, Opcodes.DMUL);

    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, INT1, Opcodes.IREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, INT2, Opcodes.IREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, INT4, Opcodes.IREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, INT8, Opcodes.LREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, FLOAT4, Opcodes.FREM);
    TUtil.putToNestedMap(OpCodesMap, EvalType.DIVIDE, FLOAT8, Opcodes.DREM);

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

  public static boolean isPrimitiveOpCode(EvalType evalType, TajoDataTypes.DataType returnType) {
    return TUtil.containsInNestedMap(OpCodesMap, evalType, returnType.getType());
  }

  public static int getOpCode(EvalType evalType, TajoDataTypes.DataType returnType) {
    if (!isPrimitiveOpCode(evalType, returnType)) {
      throw new CodeGenException("No Such OpCode for " + evalType + " returning " + returnType.getType().name());
    }
    return TUtil.getFromNestedMap(OpCodesMap, evalType, returnType.getType());
  }

  public static void insertCastInst(MethodVisitor method, TajoDataTypes.DataType srcType,
                                    TajoDataTypes.DataType targetType) {
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
        case INT8:   method.visitInsn(Opcodes.I2L); break;
        case FLOAT4: method.visitInsn(Opcodes.I2F); break;
        case FLOAT8: method.visitInsn(Opcodes.I2D); break;
        case TEXT:   addStringValueOfChar(method); break;
        default:
          throw new InvalidCastException(srcType, targetType);
        }
      } else {
        switch (targetRawType) {
        case CHAR:
        case INT1:
        case INT2:
        case INT4: addParseInt4(method); break;
        case INT8: addParseInt8(method); break;
        case FLOAT4: addParseFloat4(method); break;
        case FLOAT8: addParseFloat8(method); break;
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
      case INT1: method.visitInsn(Opcodes.I2C); break;
      case INT2: method.visitInsn(Opcodes.I2S); break;
      case INT4: return;
      case INT8: method.visitInsn(Opcodes.I2L); break;
      case FLOAT4: method.visitInsn(Opcodes.I2F); break;
      case FLOAT8: method.visitInsn(Opcodes.I2D); break;
      case TEXT: addStringValueOfInt4(method); break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case INT8:
      switch (targetRawType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: method.visitInsn(Opcodes.L2I); break;
      case INT8: return;
      case FLOAT4: method.visitInsn(Opcodes.L2F); break;
      case FLOAT8: method.visitInsn(Opcodes.L2F); break;
      case TEXT: addStringValueOfInt8(method); break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case FLOAT4:
      switch (targetRawType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: method.visitInsn(Opcodes.F2I); break;
      case INT8: method.visitInsn(Opcodes.F2L); break;
      case FLOAT4: return;
      case FLOAT8: method.visitInsn(Opcodes.F2D); break;
      case TEXT: addStringValueOfFloat4(method); break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case FLOAT8:
      switch (targetRawType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: method.visitInsn(Opcodes.D2I); break;
      case INT8: method.visitInsn(Opcodes.D2L); break;
      case FLOAT4: method.visitInsn(Opcodes.D2F); break;
      case FLOAT8: return;
      case TEXT: addStringValueOfFloat8(method); break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case TEXT:
      switch (targetRawType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: addParseInt4(method); break;
      case INT8: addParseInt8(method); break;
      case FLOAT4: addParseFloat4(method); break;
      case FLOAT8: addParseFloat8(method); break;
      case TEXT: break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    default: throw new InvalidCastException(srcType, targetType);
    }
  }

  public static String getInternalName(Class clazz) {
    return clazz.getName().replace('.', '/');
  }

  public static void addStringValueOfChar(MethodVisitor method) {
    method.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(C)L" + Type.getInternalName(String.class) + ";");
  }

  public static void addStringValueOfInt4(MethodVisitor method) {
    method.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(I)L" + Type.getInternalName(String.class) + ";");
  }

  public static void addStringValueOfInt8(MethodVisitor method) {
    method.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(J)L" + Type.getInternalName(String.class) + ";");
  }

  public static void addStringValueOfFloat4(MethodVisitor method) {
    method.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(F)L" + Type.getInternalName(String.class) + ";");
  }

  public static void addStringValueOfFloat8(MethodVisitor method) {
    method.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(String.class),
        "valueOf", "(D)L" + Type.getInternalName(String.class) + ";");
  }

  public static void addParseInt4(MethodVisitor method) {
    method.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Integer.class),
        "parseInt", "(L" + Type.getInternalName(String.class) + ";)I");
  }

  public static void addParseInt8(MethodVisitor method) {
    method.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Integer.class),
        "parseLong", "(L" + Type.getInternalName(String.class) + ";)J");
  }

  public static void addParseFloat4(MethodVisitor method) {
    method.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Float.class),
        "parseFloat", "(L" + Type.getInternalName(String.class) + ";)F");
  }

  public static void addParseFloat8(MethodVisitor method) {
    method.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getInternalName(Integer.class),
        "parseDouble", "(L" + Type.getInternalName(String.class) + ";)D");
  }
}
