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

  public static Column convert(Schema.NamedType type) {
    if (type instanceof NamedStructType) {
      NamedStructType structType = (NamedStructType) type;
      Collection<Column> converted = Collections2
          .transform(structType.fields(), new Function<Schema.NamedType, Column>() {
        @Override
        public Column apply(@Nullable Schema.NamedType namedType) {
          return FieldConverter.convert(namedType);
        }
      });
      return new Column(type.name().raw(IdentifierPolicy.DefaultPolicy()), new TypeDesc(new SchemaLegacy(converted)));
    } else {
      return new Column(type.name().displayString(IdentifierPolicy.DefaultPolicy()), TypeConverter.convert(type));
    }
  }
}
