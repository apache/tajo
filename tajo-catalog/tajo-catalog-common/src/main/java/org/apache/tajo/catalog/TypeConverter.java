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
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.schema.IdentifierPolicy;
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

  /**
   * This is for base types.
   *
   * @param legacyBaseType legacy base type
   * @return Type
   */
  public static Type convert(TajoDataTypes.Type legacyBaseType) {
    switch (legacyBaseType) {
    case BOOLEAN:
      return Bool();
    case INT1:
    case INT2:
      return Int2();
    case INT4:
      return Int4();
    case INT8:
      return Int8();
    case FLOAT4:
      return Float4();
    case FLOAT8:
      return Float8();
    case DATE:
      return Date();
    case TIME:
      return Time();
    case TIMESTAMP:
      return Timestamp();
    case INTERVAL:
      return Interval();
    case TEXT:
      return Text();
    case BLOB:
      return Blob();
    case INET4:
      return Inet4();
    case NULL_TYPE:
      return Null();
    case ANY:
      return Any();
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
    return CatalogUtil.newSimpleDataType(type.baseType());
  }

  public static TypeDesc convert(Schema.NamedType src) {
    if (src instanceof Schema.NamedStructType) {
      Schema.NamedStructType structType = (Schema.NamedStructType) src;

      ImmutableList.Builder<Column> fields = ImmutableList.builder();
      for (Schema.NamedType t: structType.fields()) {
        fields.add(new Column(t.name().raw(IdentifierPolicy.DefaultPolicy()), convert(t)));
      }

      return new TypeDesc(SchemaBuilder.builder().addAll(fields.build()).build());
    } else {
      final Schema.NamedPrimitiveType namedType = (Schema.NamedPrimitiveType) src;
      final Type type = namedType.type();
      if (type instanceof Char) {
        Char charType = (Char) type;
        return new TypeDesc(CatalogUtil.newDataTypeWithLen(TajoDataTypes.Type.CHAR, charType.length()));
      } else if (type instanceof Varchar) {
        Varchar varcharType = (Varchar) type;
        return new TypeDesc(CatalogUtil.newDataTypeWithLen(TajoDataTypes.Type.VARCHAR, varcharType.length()));
      } else if (type instanceof Numeric) {
        Numeric numericType = (Numeric) type;
        return new TypeDesc(CatalogUtil.newDataTypeWithLen(TajoDataTypes.Type.NUMERIC, numericType.precision()));
      } else if (type instanceof Protobuf) {
        Protobuf protobuf = (Protobuf) type;
        return new TypeDesc(CatalogUtil.newDataType(TajoDataTypes.Type.PROTOBUF, protobuf.getMessageName()));
      } else {
        return new TypeDesc(convert(namedType.type()));
      }
    }
  }
}
