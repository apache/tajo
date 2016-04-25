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

package org.apache.tajo.catalog;

import com.google.common.collect.ImmutableList;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.schema.Schema;
import org.apache.tajo.type.*;

import java.util.Collection;

import static org.apache.tajo.type.Type.*;

public class TypeConverter {

  public static Collection<Schema.NamedType> convert(TypeDesc type) {
    ImmutableList.Builder<Schema.NamedType> fields = ImmutableList.builder();
    for (Column c : type.getNestedSchema().getRootColumns()) {
      fields.add(FieldConverter.convert(c));
    }
    return fields.build();
  }

  public static Type convert(TajoDataTypes.Type legacyBaseType) {
    switch (legacyBaseType) {
    case BOOLEAN:
      return Bool;
    case INT1:
      return Int1;
    case INT2:
      return Int2;
    case INT4:
      return Int4;
    case INT8:
      return Int8;
    case FLOAT4:
      return Float4;
    case FLOAT8:
      return Float8;
    case DATE:
      return Date;
    case TIME:
      return Time;
    case TIMESTAMP:
      return Timestamp;
    case INTERVAL:
      return Interval;
    case CHAR:
      return Char(1); // default len = 1
    case TEXT:
      return Text;
    case BLOB:
      return Blob;
    case INET4:
      return Inet4;
    case RECORD:
      throw new TajoRuntimeException(new NotImplementedException("record projection"));
    case NULL_TYPE:
      return Null;
    case ANY:
      return Any;
    default:
      throw new TajoRuntimeException(new UnsupportedException(legacyBaseType.name()));
    }
  }

  public static Type convert(TajoDataTypes.DataType legacyType) {
    switch (legacyType.getType()) {
    case NCHAR:
    case CHAR:
      return Char(legacyType.getLength());
    case NVARCHAR:
    case VARCHAR:
      return Varchar(legacyType.getLength());
    case NUMERIC:
      return Numeric(legacyType.getLength());
    case PROTOBUF:
      return new Protobuf(legacyType.getCode());
    default:
      return convert(legacyType.getType());
    }
  }

  public static TajoDataTypes.DataType convert(Type type) {
    switch (type.baseType()) {
      case CHAR:
        Char charType = (Char) type;
        return CatalogUtil.newDataTypeWithLen(TajoDataTypes.Type.CHAR, charType.length());
      case VARCHAR:
        Varchar varcharType = (Varchar) type;
        return CatalogUtil.newDataTypeWithLen(TajoDataTypes.Type.VARCHAR, varcharType.length());
      case PROTOBUF:
        Protobuf protobuf = (Protobuf) type;
        return CatalogUtil.newDataType(TajoDataTypes.Type.PROTOBUF, protobuf.getMessageName());
      case NUMERIC:
        Numeric numericType = (Numeric) type;
        return CatalogUtil.newDataTypeWithLen(TajoDataTypes.Type.NUMERIC, numericType.precision());
      default:
        return CatalogUtil.newSimpleDataType(type.baseType());
    }
  }
}
