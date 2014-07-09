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

package org.apache.tajo.engine.utils;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.eval.InvalidEvalException;

public class DataTypeUtil {
  /**
   * This is verified by ExprsVerifier.checkArithmeticOperand().
   */
  public static TajoDataTypes.DataType determineType(TajoDataTypes.DataType left, TajoDataTypes.DataType right) throws InvalidEvalException {
    switch (left.getType()) {

    case INT1:
    case INT2:
    case INT4: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4);
      case INT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8);
      case FLOAT4: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT4);
      case FLOAT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
      case DATE: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.DATE);
      case INTERVAL: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INTERVAL);
      }
    }

    case INT8: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4:
      case INT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8);
      case FLOAT4: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT4);
      case FLOAT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
      case DATE: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.DATE);
      case INTERVAL: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INTERVAL);
      }
    }

    case FLOAT4: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT4);
      case INT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT4);
      case FLOAT4: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT4);
      case FLOAT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
      case INTERVAL: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INTERVAL);
      }
    }

    case FLOAT8: {
      switch(right.getType()) {
      case INT1:
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
      case INTERVAL: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INTERVAL);
      }
    }

    case DATE: {
      switch(right.getType()) {
      case INT2:
      case INT4:
      case INT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.DATE);
      case INTERVAL:
      case TIME: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TIMESTAMP);
      case DATE: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4);
      }
    }

    case TIME: {
      switch(right.getType()) {
      case INTERVAL: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TIME);
      case TIME: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INTERVAL);
      case DATE: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4);
      }
    }

    case TIMESTAMP: {
      switch (right.getType()) {
      case INTERVAL: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TIMESTAMP);
      case TIMESTAMP: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INTERVAL);
      }
    }

    case INTERVAL: {
      switch (right.getType()) {
      case INTERVAL:
      case FLOAT4:
      case FLOAT8: return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INTERVAL);
      }
    }

    default: return left;
    }
  }
}
