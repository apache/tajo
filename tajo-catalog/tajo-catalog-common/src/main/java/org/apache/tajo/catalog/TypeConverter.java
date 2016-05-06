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
import org.apache.tajo.schema.Field;
import org.apache.tajo.type.*;

import static org.apache.tajo.catalog.CatalogUtil.newDataTypeWithLen;
import static org.apache.tajo.catalog.CatalogUtil.newSimpleDataType;
import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.apache.tajo.type.Type.*;

public class TypeConverter {

  public static Type convert(TypeDesc type) {
    if (type.getDataType().getType() == TajoDataTypes.Type.RECORD) {
      ImmutableList.Builder<Field> fields = ImmutableList.builder();
      for (Column c : type.getNestedSchema().getRootColumns()) {
        fields.add(FieldConverter.convert(c));
      }
      return Record(fields.build());
    } else {
      return convert(type.dataType);
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

  /**
   * This is for base types.
   *
   * @param legacyBaseType legacy base type
   * @return Type
   */
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
      // for better exception
      throw new TajoRuntimeException(new NotImplementedException("record projection"));
    case NULL_TYPE:
      return Null;
    case ANY:
      return Any;

    case BOOLEAN_ARRAY:
      return Array(Bool);
    case INT1_ARRAY:
      return Array(Int1);
    case INT2_ARRAY:
      return Array(Int2);
    case INT4_ARRAY:
      return Array(Int4);
    case INT8_ARRAY:
      return Array(Int8);
    case FLOAT4_ARRAY:
      return Array(Float4);
    case FLOAT8_ARRAY:
      return Array(Float8);
    case TIMESTAMP_ARRAY:
      return Array(Timestamp);
    case DATE_ARRAY:
      return Array(Date);
    case TIME_ARRAY:
      return Array(Time);
    case TEXT_ARRAY:
      return Array(Text);

    default:
      throw new TajoRuntimeException(new UnsupportedException(legacyBaseType.name()));
    }
  }

  public static TypeDesc convert(Field src) {
    return convert(src.type());
  }

  public static TypeDesc convert(Type type) {
    switch (type.baseType()) {
    case CHAR:
      Char charType = (Char) type;
      return new TypeDesc(newDataTypeWithLen(TajoDataTypes.Type.CHAR, charType.length()));
    case VARCHAR:
      Varchar varcharType = (Varchar) type;
      return new TypeDesc(newDataTypeWithLen(TajoDataTypes.Type.VARCHAR, varcharType.length()));
    case NUMERIC:
      Numeric numericType = (Numeric) type;
      return new TypeDesc(newDataTypeWithLen(TajoDataTypes.Type.NUMERIC, numericType.precision()));
    case PROTOBUF:
      Protobuf protobuf = (Protobuf) type;
      return new TypeDesc(CatalogUtil.newDataType(TajoDataTypes.Type.PROTOBUF, protobuf.getMessageName()));
    case RECORD:
      Record record = (Record) type;
      ImmutableList.Builder<Column> fields = ImmutableList.builder();
      for (Field t: record.fields()) {
        fields.add(new Column(t.name().interned(), convert(t)));
      }
      return new TypeDesc(SchemaBuilder.builder().addAll(fields.build()).build());

    case ARRAY:
      Array array = (Array) type;
      Type elemType = array.elementType();
      switch (elemType.baseType()) {
      case INT1:
        return new TypeDesc(newSimpleDataType(INT1_ARRAY));
      case INT2:
        return new TypeDesc(newSimpleDataType(INT2_ARRAY));
      case INT4:
        return new TypeDesc(newSimpleDataType(INT4_ARRAY));
      case INT8:
        return new TypeDesc(newSimpleDataType(INT8_ARRAY));
      case FLOAT4:
        return new TypeDesc(newSimpleDataType(FLOAT4_ARRAY));
      case FLOAT8:
        return new TypeDesc(newSimpleDataType(FLOAT8_ARRAY));
      default:
        return new TypeDesc(newSimpleDataType(type.baseType()));
      }

    default:
      return new TypeDesc(newSimpleDataType(type.baseType()));
    }
  }
}
