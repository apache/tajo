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
import org.apache.tajo.schema.QualifiedIdentifier;
import org.apache.tajo.schema.Schema.NamedPrimitiveType;
import org.apache.tajo.schema.Schema.NamedStructType;
import org.apache.tajo.schema.Schema.NamedType;
import org.apache.tajo.type.Type;

import java.util.Collection;
import java.util.Iterator;

import static org.apache.tajo.catalog.FieldConverter.toQualifiedIdentifier;
import static org.apache.tajo.schema.IdentifierPolicy.DefaultPolicy;

/**
 * Builder for Schema
 */
public class SchemaBuilder {
  private final ImmutableList.Builder<NamedType> fields = new ImmutableList.Builder();

  public SchemaBuilder() {}

  public SchemaBuilder(Iterator<NamedType> fields) {
    this.fields.addAll(fields);
  }

  public SchemaBuilder(Iterable<NamedType> fields) {
    this.fields.addAll(fields);
  }

  public SchemaBuilder add(NamedType namedType) {
    fields.add(namedType);
    return this;
  }

  public SchemaBuilder add(QualifiedIdentifier id, Type type) {
    add(new NamedPrimitiveType(id, type));
    return this;
  }

  public SchemaBuilder addStruct(QualifiedIdentifier id, Collection<NamedType> fields) {
    add(new NamedStructType(id, fields));
    return this;
  }

  @Deprecated
  public SchemaBuilder add(String name, TypeDesc legacyType) {
    if (legacyType.getDataType().getType() == TajoDataTypes.Type.RECORD) {
      addStruct(toQualifiedIdentifier(name), TypeConverter.convert(legacyType));
    } else {
      add(toQualifiedIdentifier(name), TypeConverter.convert(legacyType.getDataType()));
    }
    return this;
  }

  @Deprecated
  public SchemaBuilder add(String name, TajoDataTypes.DataType dataType) {
    add(name, new TypeDesc(dataType));
    return this;
  }

  @Deprecated
  public SchemaBuilder add(String name, TajoDataTypes.Type baseType) {
    add(name, new TypeDesc(CatalogUtil.newSimpleDataType(baseType)));
    return this;
  }

  @Deprecated
  public SchemaBuilder add(Column column) {
    add(FieldConverter.convert(column));
    return this;
  }

  @Deprecated
  public SchemaBuilder addAll(Iterable<Column> columns) {
    for (Column c :columns) {
      add(c);
    }
    return this;
  }

  @Deprecated
  public SchemaLegacy buildV1() {
    ImmutableList.Builder<Column> columns = new ImmutableList.Builder();
    for (NamedType namedType : fields.build()) {
      columns.add(new Column(namedType.name().raw(DefaultPolicy()), FieldConverter.convert(namedType)));
    }

    return new SchemaLegacy(columns.build());
  }

  public org.apache.tajo.schema.Schema buildV2() {
    return new org.apache.tajo.schema.Schema(fields.build());
  }
}
