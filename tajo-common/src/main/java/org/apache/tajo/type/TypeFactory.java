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

package org.apache.tajo.type;

import org.apache.tajo.Assert;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.schema.Field;
import org.apache.tajo.util.StringUtils;

import java.util.List;

import static org.apache.tajo.type.Type.Char;
import static org.apache.tajo.type.Type.Null;
import static org.apache.tajo.type.Type.Varchar;

public class TypeFactory {
  /**
   * This is for base types.
   *
   * @param baseType legacy base type
   * @return Type
   */
  public static Type create(TajoDataTypes.Type baseType) {
    switch (baseType) {
    case BOOLEAN:
      return Type.Bool;
    case INT1:
      return Type.Int1;
    case INT2:
      return Type.Int2;
    case INT4:
      return Type.Int4;
    case INT8:
      return Type.Int8;
    case FLOAT4:
      return Type.Float4;
    case FLOAT8:
      return Type.Float8;
    case DATE:
      return Type.Date;
    case TIME:
      return Type.Time;
    case TIMESTAMP:
      return Type.Timestamp;
    case INTERVAL:
      return Type.Interval;
    case CHAR:
      return Type.Char(1); // default len = 1
    case TEXT:
      return Type.Text;
    case BLOB:
      return Type.Blob;
    case RECORD:
      // for better exception
      throw new TajoRuntimeException(new NotImplementedException("record projection"));
    case NULL_TYPE:
      return Type.Null;
    case ANY:
      return Type.Any;

    case BOOLEAN_ARRAY:
      return Type.Array(Type.Bool);
    case INT1_ARRAY:
      return Type.Array(Type.Int1);
    case INT2_ARRAY:
      return Type.Array(Type.Int2);
    case INT4_ARRAY:
      return Type.Array(Type.Int4);
    case INT8_ARRAY:
      return Type.Array(Type.Int8);
    case FLOAT4_ARRAY:
      return Type.Array(Type.Float4);
    case FLOAT8_ARRAY:
      return Type.Array(Type.Float8);
    case TIMESTAMP_ARRAY:
      return Type.Array(Type.Timestamp);
    case DATE_ARRAY:
      return Type.Array(Type.Date);
    case TIME_ARRAY:
      return Type.Array(Type.Time);
    case TEXT_ARRAY:
      return Type.Array(Type.Text);

    default:
      throw new TajoRuntimeException(new UnsupportedException(baseType.name()));
    }
  }

  public static Type create(TajoDataTypes.Type baseType,
                            List<Type> typeParams,
                            List<Integer> valueParams,
                            List<Field> fieldParams) {
    switch (baseType) {
    case CHAR: {
      Assert.assertCondition(valueParams.size() == 1,
          "Char type requires 1 integer parameters, but it takes (%s).", StringUtils.join(typeParams));
      return Char(valueParams.get(0));
    }
    case VARCHAR: {
      Assert.assertCondition(valueParams.size() == 1,
          "Varchar type requires 1 integer parameters, but it takes (%s).", StringUtils.join(typeParams));
      return Varchar(valueParams.get(0));
    }
    case TEXT: return Type.Text;

    case BOOLEAN: return Type.Bool;
    case INT1: return Type.Int1;
    case INT2: return Type.Int2;
    case INT4: return Type.Int4;
    case INT8: return Type.Int8;
    case FLOAT4: return Type.Float4;
    case FLOAT8: return Type.Float8;
    case NUMERIC: {
      if (valueParams.size() == 0) {
        return Type.Numeric;

      } else {
        for (Object p : valueParams) {
          Assert.assertCondition(p instanceof Integer, "Numeric type requires integer parameters");
        }
        if (valueParams.size() == 1) {
          return Numeric.Numeric(valueParams.get(0));
        } else if (valueParams.size() == 2) {
          return Numeric.Numeric(valueParams.get(0), valueParams.get(1));
        } else {
          Assert.assertCondition(false,
              "Numeric type can take 2 or less integer parameters, but it takes (%s).", StringUtils.join(valueParams));
        }
      }
    }

    case DATE: return Type.Date;
    case TIME: return Type.Time;
    case TIMESTAMP: return Type.Timestamp;
    case INTERVAL: return Type.Interval;
    case BLOB: return Type.Blob;

    case ARRAY: {
      Assert.assertCondition(typeParams.size() == 1,
          "Array Type requires 1 type parameters, but it takes (%s).", StringUtils.join(typeParams));
      return Type.Array(typeParams.get(0));
    }
    case RECORD: {
      Assert.assertCondition(fieldParams.size() >= 1,
          "Record Type requires at least 1 field parameters, but it takes (%s).", StringUtils.join(fieldParams));
      return Type.Record(fieldParams);
    }
    case MAP: {
      Assert.assertCondition(typeParams.size() == 2,
          "Map Type requires 2 type parameters, but it takes (%s).", StringUtils.join(typeParams));

      return Type.Map(typeParams.get(0), typeParams.get(1));
    }
    case NULL_TYPE:
      return Null();

    default:
      throw new TajoInternalError(new UnsupportedException(baseType.name()));
    }
  }
}
