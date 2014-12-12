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

package org.apache.tajo.storage.parquet;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts between Parquet and Tajo schemas. See package documentation for
 * details on the mapping.
 */
public class TajoSchemaConverter {
  private static final String TABLE_SCHEMA = "table_schema";

  /**
   * Creates a new TajoSchemaConverter.
   */
  public TajoSchemaConverter() {
  }

  /**
   * Converts a Parquet schema to a Tajo schema.
   *
   * @param parquetSchema The Parquet schema to convert.
   * @return The resulting Tajo schema.
   */
  public Schema convert(MessageType parquetSchema) {
    return convertFields(parquetSchema.getFields());
  }

  private Schema convertFields(List<Type> parquetFields) {
    List<Column> columns = new ArrayList<Column>();
    for (int i = 0; i < parquetFields.size(); ++i) {
      Type fieldType = parquetFields.get(i);
      if (fieldType.isRepetition(Type.Repetition.REPEATED)) {
        throw new RuntimeException("REPEATED not supported outside LIST or" +
            " MAP. Type: " + fieldType);
      }
      columns.add(convertField(fieldType));
    }
    Column[] columnsArray = new Column[columns.size()];
    columnsArray = columns.toArray(columnsArray);
    return new Schema(columnsArray);
  }

  private Column convertField(final Type fieldType) {
    if (fieldType.isPrimitive()) {
      return convertPrimitiveField(fieldType);
    } else {
      return convertComplexField(fieldType);
    }
  }

  private Column convertPrimitiveField(final Type fieldType) {
    final String fieldName = fieldType.getName();
    final PrimitiveTypeName parquetPrimitiveTypeName =
        fieldType.asPrimitiveType().getPrimitiveTypeName();
    final OriginalType originalType = fieldType.getOriginalType();
    return parquetPrimitiveTypeName.convert(
        new PrimitiveType.PrimitiveTypeNameConverter<Column, RuntimeException>() {
      @Override
      public Column convertBOOLEAN(PrimitiveTypeName primitiveTypeName) {
        return new Column(fieldName, TajoDataTypes.Type.BOOLEAN);
      }

      @Override
      public Column convertINT32(PrimitiveTypeName primitiveTypeName) {
        return new Column(fieldName, TajoDataTypes.Type.INT4);
      }

      @Override
      public Column convertINT64(PrimitiveTypeName primitiveTypeName) {
        return new Column(fieldName, TajoDataTypes.Type.INT8);
      }

      @Override
      public Column convertFLOAT(PrimitiveTypeName primitiveTypeName) {
        return new Column(fieldName, TajoDataTypes.Type.FLOAT4);
      }

      @Override
      public Column convertDOUBLE(PrimitiveTypeName primitiveTypeName) {
        return new Column(fieldName, TajoDataTypes.Type.FLOAT8);
      }

      @Override
      public Column convertFIXED_LEN_BYTE_ARRAY(
          PrimitiveTypeName primitiveTypeName) {
        return new Column(fieldName, TajoDataTypes.Type.BLOB);
      }

      @Override
      public Column convertBINARY(PrimitiveTypeName primitiveTypeName) {
        if (originalType == OriginalType.UTF8) {
          return new Column(fieldName, TajoDataTypes.Type.TEXT);
        } else {
          return new Column(fieldName, TajoDataTypes.Type.BLOB);
        }
      }

      @Override
      public Column convertINT96(PrimitiveTypeName primitiveTypeName) {
        throw new RuntimeException("Converting from INT96 not supported.");
      }
    });
  }

  private Column convertComplexField(final Type fieldType) {
    throw new RuntimeException("Complex types not supported.");
  }

  /**
   * Converts a Tajo schema to a Parquet schema.
   *
   * @param tajoSchema The Tajo schema to convert.
   * @return The resulting Parquet schema.
   */
  public MessageType convert(Schema tajoSchema) {
    List<Type> types = new ArrayList<Type>();
    for (int i = 0; i < tajoSchema.size(); ++i) {
      Column column = tajoSchema.getColumn(i);
      if (column.getDataType().getType() == TajoDataTypes.Type.NULL_TYPE) {
        continue;
      }
      types.add(convertColumn(column));
    }
    return new MessageType(TABLE_SCHEMA, types);
  }

  private Type convertColumn(Column column) {
    TajoDataTypes.Type type = column.getDataType().getType();
    switch (type) {
      case BOOLEAN:
        return primitive(column.getSimpleName(),
                         PrimitiveTypeName.BOOLEAN);
      case BIT:
      case INT2:
      case INT4:
        return primitive(column.getSimpleName(),
                         PrimitiveTypeName.INT32);
      case INT8:
        return primitive(column.getSimpleName(),
                         PrimitiveTypeName.INT64);
      case FLOAT4:
        return primitive(column.getSimpleName(),
                         PrimitiveTypeName.FLOAT);
      case FLOAT8:
        return primitive(column.getSimpleName(),
                         PrimitiveTypeName.DOUBLE);
      case CHAR:
      case TEXT:
        return primitive(column.getSimpleName(),
                         PrimitiveTypeName.BINARY,
                         OriginalType.UTF8);
      case PROTOBUF:
        return primitive(column.getSimpleName(),
                         PrimitiveTypeName.BINARY);
      case BLOB:
        return primitive(column.getSimpleName(),
                         PrimitiveTypeName.BINARY);
      case INET4:
      case INET6:
        return primitive(column.getSimpleName(),
                         PrimitiveTypeName.BINARY);
      default:
        throw new RuntimeException("Cannot convert Tajo type: " + type);
    }
  }

  private PrimitiveType primitive(String name,
                                  PrimitiveTypeName primitive) {
    return new PrimitiveType(Type.Repetition.OPTIONAL, primitive, name, null);
  }

  private PrimitiveType primitive(String name,
                                  PrimitiveTypeName primitive,
                                  OriginalType originalType) {
    return new PrimitiveType(Type.Repetition.OPTIONAL, primitive, name,
                             originalType);
  }
}
