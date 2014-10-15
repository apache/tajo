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
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.util.TUtil;

import java.util.Map;

import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.apache.tajo.common.TajoDataTypes.Type.*;

public class DataTypeUtil {

  public static final Map<Type, Map<Type, Boolean>> FUNCTION_ACCEPTABLE_PARAM_MAP = Maps.newHashMap();

  private static void putAcceptableType(Type given, Type define) {
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

    putAcceptableType(INET4, INET4);
  }

  public static boolean isUpperCastable(Type define, Type given) {
    if (given == define) {
      return true;
    }

    return TUtil.containsInNestedMap(FUNCTION_ACCEPTABLE_PARAM_MAP, given, define);
  }

  /**
   * This is verified by ExprsVerifier.checkArithmeticOperand().
   */
  public static TajoDataTypes.DataType determineType(TajoDataTypes.DataType left, TajoDataTypes.DataType right) {
    switch (left.getType()) {

    case INT1:
    case INT2:
    case INT4: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4: return CatalogUtil.newSimpleDataType(Type.INT4);
      case INT8: return CatalogUtil.newSimpleDataType(Type.INT8);
      case FLOAT4: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.FLOAT8);
      case DATE: return CatalogUtil.newSimpleDataType(Type.DATE);
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case INT8: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4:
      case INT8: return CatalogUtil.newSimpleDataType(Type.INT8);
      case FLOAT4: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.FLOAT8);
      case DATE: return CatalogUtil.newSimpleDataType(Type.DATE);
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case FLOAT4: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case INT8: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case FLOAT4: return CatalogUtil.newSimpleDataType(Type.FLOAT4);
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.FLOAT8);
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case FLOAT8: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.FLOAT8);
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case DATE: {
      switch(right.getType()) {
      case INT2:
      case INT4:
      case INT8: return CatalogUtil.newSimpleDataType(Type.DATE);
      case INTERVAL:
      case TIME: return CatalogUtil.newSimpleDataType(Type.TIMESTAMP);
      case DATE: return CatalogUtil.newSimpleDataType(Type.INT4);
      }
    }

    case TIME: {
      switch(right.getType()) {
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.TIME);
      case TIME: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      case DATE: return CatalogUtil.newSimpleDataType(Type.INT4);
      }
    }

    case TIMESTAMP: {
      switch (right.getType()) {
      case INTERVAL: return CatalogUtil.newSimpleDataType(Type.TIMESTAMP);
      case TIMESTAMP: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    case INTERVAL: {
      switch (right.getType()) {
      case INTERVAL:
      case FLOAT4:
      case FLOAT8: return CatalogUtil.newSimpleDataType(Type.INTERVAL);
      }
    }

    default: return left;
    }
  }
}
