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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.schema.Identifier;
import org.apache.tajo.schema.IdentifierPolicy;
import org.apache.tajo.schema.QualifiedIdentifier;
import org.apache.tajo.schema.Schema;
import org.apache.tajo.schema.Schema.NamedPrimitiveType;
import org.apache.tajo.schema.Schema.NamedStructType;
import org.apache.tajo.type.*;

import javax.annotation.Nullable;
import java.util.Collection;

public class FieldConverter {

  public static QualifiedIdentifier toQualifiedIdentifier(String name) {
    final Collection<String> elems = ImmutableList.copyOf(name.split("\\."));
    final Collection<Identifier> identifiers = Collections2.transform(elems, new Function<String, Identifier>() {
      @Override
      public Identifier apply(@Nullable String input) {
        boolean needQuote = CatalogUtil.isShouldBeQuoted(input);
        return Identifier._(input, needQuote);
      }
    });
    return QualifiedIdentifier.$(identifiers);
  }

  public static TypeDesc convert(Schema.NamedType src) {
    if (src instanceof NamedStructType) {
      NamedStructType structType = (NamedStructType) src;

      ImmutableList.Builder<Column> fields = ImmutableList.builder();
      for (Schema.NamedType t: structType.fields()) {
        fields.add(new Column(t.name().raw(IdentifierPolicy.DefaultPolicy()), convert(t)));
      }

      return new TypeDesc(SchemaBuilder.builder().addAll(fields.build()).build());
    } else {
      final NamedPrimitiveType namedType = (NamedPrimitiveType) src;
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
        return new TypeDesc(TypeConverter.convert(namedType.type()));
      }
    }
  }

  public static Schema.NamedType convert(Column column) {
    if (column.getTypeDesc().getDataType().getType() == TajoDataTypes.Type.RECORD) {

      if (column.getTypeDesc().getNestedSchema() == null) {
        throw new TajoRuntimeException(new NotImplementedException("record type projection"));
      }

      return new NamedStructType(toQualifiedIdentifier(column.getQualifiedName()),
          TypeConverter.convert(column.getTypeDesc()));

    } else {
      return new NamedPrimitiveType(toQualifiedIdentifier(column.getQualifiedName()),
          TypeConverter.convert(column.getDataType())
      );
    }
  }
}
