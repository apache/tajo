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

package org.apache.tajo;

import com.google.common.collect.Maps;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.util.TUtil;

import java.util.Map;

import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.apache.tajo.type.Type.*;

public class DataTypeUtil {

  public static final Map<TajoDataTypes.Type, Map<TajoDataTypes.Type, Boolean>> FUNCTION_ACCEPTABLE_PARAM_MAP = Maps.newHashMap();

  private static void putAcceptableType(TajoDataTypes.Type given, TajoDataTypes.Type define) {
    TUtil.putToNestedMap(FUNCTION_ACCEPTABLE_PARAM_MAP, given, define, true);
  }

  static {
    putAcceptableType(BOOLEAN, BOOLEAN);

    putAcceptableType(INT1, INT1);
    putAcceptableType(INT1, INT2);
    putAcceptableType(INT1, INT4);
    putAcceptableType(INT1, INT8);
    putAcceptableType(INT1, FLOAT4);
    putAcceptableType(INT1, FLOAT8);

    putAcceptableType(INT2, INT2);
    putAcceptableType(INT2, INT4);
    putAcceptableType(INT2, INT8);
    putAcceptableType(INT2, FLOAT4);
    putAcceptableType(INT2, FLOAT8);

    putAcceptableType(INT4, INT4);
    putAcceptableType(INT4, INT8);
    putAcceptableType(INT4, FLOAT4);
    putAcceptableType(INT4, FLOAT8);

    putAcceptableType(INT8, INT8);
    putAcceptableType(INT8, FLOAT4);
    putAcceptableType(INT8, FLOAT8);

    putAcceptableType(FLOAT4, FLOAT4);
    putAcceptableType(FLOAT4, FLOAT8);

    putAcceptableType(FLOAT8, FLOAT8);

    putAcceptableType(TIMESTAMP, TIMESTAMP);
    putAcceptableType(TIME, TIME);
    putAcceptableType(DATE, DATE);

    putAcceptableType(TEXT, TEXT);
  }

  public static boolean isUpperCastable(TajoDataTypes.Type define, TajoDataTypes.Type given) {
    if (given.equals(define)) {
      return true;
    }

    return TUtil.containsInNestedMap(FUNCTION_ACCEPTABLE_PARAM_MAP, given, define);
  }

  /**
   * This is verified by ExprsVerifier.checkArithmeticOperand().
   */
  public static org.apache.tajo.type.Type determineType(org.apache.tajo.type.Type left,
                                                        org.apache.tajo.type.Type right) {
    TajoDataTypes.Type rhsBaseType = right.kind();
    switch (left.kind()) {

    case INT1:
    case INT2:
    case INT4: {
      switch(rhsBaseType) {
      case INT1:
      case INT2:
      case INT4: return Int4;
      case INT8: return Int8;
      case FLOAT4: return Float4;
      case FLOAT8: return Float8;
      case DATE: return Date;
      case INTERVAL: return Interval;
      }
    }

    case INT8: {
      switch(rhsBaseType) {
      case INT1:
      case INT2:
      case INT4:
      case INT8: return Int8;
      case FLOAT4: return Float4;
      case FLOAT8: return Float8;
      case DATE: return Date;
      case INTERVAL: return Interval;
      }
    }

    case FLOAT4: {
      switch(rhsBaseType) {
      case INT1:
      case INT2:
      case INT4: return Float4;
      case INT8: return Float4;
      case FLOAT4: return Float4;
      case FLOAT8: return Float8;
      case INTERVAL: return Interval;
      }
    }

    case FLOAT8: {
      switch(rhsBaseType) {
      case INT1:
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8: return Float8;
      case INTERVAL: return Interval;
      }
    }

    case DATE: {
      switch(rhsBaseType) {
      case INT2:
      case INT4:
      case INT8: return Date;
      case INTERVAL:
      case TIME: return Timestamp;
      case DATE: return Int4;
      }
    }

    case TIME: {
      switch(rhsBaseType) {
      case INTERVAL: return Time;
      case TIME: return Interval;
      case DATE: return Int4;
      }
    }

    case TIMESTAMP: {
      switch (rhsBaseType) {
      case INTERVAL: return Timestamp;
      case TIMESTAMP: return Interval;
      }
    }

    case INTERVAL: {
      switch (rhsBaseType) {
      case INTERVAL:
      case FLOAT4:
      case FLOAT8: return Interval;
      }
    }

    default: return left;
    }
  }
}
