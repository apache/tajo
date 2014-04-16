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

package org.apache.tajo.util;

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidCastException;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class CodeGenUtil {

  public static void insertCastInst(MethodVisitor method, TajoDataTypes.Type srcType, TajoDataTypes.Type targetType) {
    Integer opCode = null;

    switch(srcType) {
    case BIT:
    case CHAR:
    case INT1:
    case INT2:
    case INT4:
      switch (targetType) {
      case CHAR:
      case INT1: opCode = Opcodes.I2C; break;
      case INT2: opCode = Opcodes.I2S; break;
      case INT4: break;
      case INT8: opCode = Opcodes.I2L; break;
      case FLOAT4: opCode = Opcodes.I2F; break;
      case FLOAT8: opCode = Opcodes.I2D; break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case INT8:
      switch (targetType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: opCode = Opcodes.L2I; break;
      case INT8: break;
      case FLOAT4: opCode = Opcodes.L2F; break;
      case FLOAT8: opCode = Opcodes.L2F; break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case FLOAT4:
      switch (targetType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: opCode = Opcodes.F2I; break;
      case INT8: opCode = Opcodes.F2L; break;
      case FLOAT4: break;
      case FLOAT8: opCode = Opcodes.F2D; break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    case FLOAT8:
      switch (targetType) {
      case CHAR:
      case INT1:
      case INT2:
      case INT4: opCode = Opcodes.D2I; break;
      case INT8: opCode = Opcodes.D2L; break;
      case FLOAT4: opCode = Opcodes.D2F; break;
      case FLOAT8: break;
      default: throw new InvalidCastException(srcType, targetType);
      }
      break;
    default: throw new InvalidCastException(srcType, targetType);
    }

    method.visitInsn(opCode);
  }
}
