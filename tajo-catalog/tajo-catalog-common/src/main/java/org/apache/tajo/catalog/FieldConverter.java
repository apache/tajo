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
import org.apache.tajo.TypeConverter;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.schema.Identifier;
import org.apache.tajo.schema.IdentifierPolicy;
import org.apache.tajo.schema.QualifiedIdentifier;
import org.apache.tajo.schema.Schema;
import org.apache.tajo.type.Type;

import javax.annotation.Nullable;
import java.util.Collection;

public class FieldConverter {

  public static TypeDesc convert(Schema.NamedType src) {
    if (src instanceof Schema.NamedStructType) {
      Schema.NamedStructType structType = (Schema.NamedStructType) src;

      ImmutableList.Builder<Column> fields = ImmutableList.builder();
      for (Schema.NamedType t: structType.fields()) {
        fields.add(new Column(t.name().displayString(IdentifierPolicy.DefaultPolicy()), convert(t)));
      }

      return new TypeDesc(SchemaFactory.newV1(new SchemaLegacy(fields.build())));
    } else {
      Schema.NamedPrimitiveType namedType = (Schema.NamedPrimitiveType) src;
      return new TypeDesc(convert(namedType.type()));
    }
  }

  public static TajoDataTypes.DataType convert(Type type) {
    return CatalogUtil.newSimpleDataType(type.baseType());
  }

  public static QualifiedIdentifier transformIdentifier(String name) {
    Collection<String> elems = ImmutableList.copyOf(name.split("\\."));
    Collection<Identifier> identifiers = Collections2.transform(elems, new Function<String, Identifier>() {
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
      ImmutableList.Builder<Schema.NamedType> fields = ImmutableList.builder();
      TypeDesc typeDesc = column.getTypeDesc();
      for (Column c :typeDesc.getNestedSchema().getRootColumns()) {
        fields.add(convert(c));
      }
      return new Schema.NamedStructType(transformIdentifier(column.getQualifiedName()), fields.build());
    } else {
      return new Schema.NamedPrimitiveType(
          transformIdentifier(column.getQualifiedName()),
          TypeConverter.convert(column.getDataType())
      );
    }
  }
}
