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

package org.apache.tajo.storage.newtuple;

import org.apache.tajo.common.TajoDataTypes;

public class TypeUtil {

  public static boolean isFixedSize(TajoDataTypes.DataType dataType) {
    boolean fixed = false;

    TajoDataTypes.Type type = dataType.getType();

    fixed |= type == TajoDataTypes.Type.BOOLEAN;
    fixed |= type == TajoDataTypes.Type.INT1;
    fixed |= type == TajoDataTypes.Type.INT2;
    fixed |= type == TajoDataTypes.Type.INT4;
    fixed |= type == TajoDataTypes.Type.INT8;
    fixed |= type == TajoDataTypes.Type.FLOAT4;
    fixed |= type == TajoDataTypes.Type.FLOAT8;
    fixed |= type == TajoDataTypes.Type.INET4;
    fixed |= type == TajoDataTypes.Type.TIMESTAMP;
    fixed |= type == TajoDataTypes.Type.DATE;
    fixed |= type == TajoDataTypes.Type.TIME;

    if (type == TajoDataTypes.Type.CHAR) {
      throw new RuntimeException("does not support: " + TajoDataTypes.Type.CHAR.name());
    }

    return fixed;
  }

  public static byte sizeOf(TajoDataTypes.DataType dataType) {
    switch (dataType.getType()) {
    case INT1:
    case INT2: return SizeOf.SIZE_OF_SHORT;
    case INT4: return SizeOf.SIZE_OF_INT;
    case INT8: return SizeOf.SIZE_OF_LONG;
    case FLOAT4: return SizeOf.SIZE_OF_FLOAT;
    case FLOAT8: return SizeOf.SIZE_OF_DOUBLE;
    default: throw new RuntimeException("does not support this type: " + dataType.getType().name());
    }
  }
}
