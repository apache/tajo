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
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.schema.Field;
import org.apache.tajo.schema.QualifiedIdentifier;
import org.apache.tajo.type.Type;

import javax.annotation.Nullable;
import java.util.Iterator;

import static org.apache.tajo.catalog.FieldConverter.toQualifiedIdentifier;

/**
 * Builder for Schema
 */
public class SchemaBuilder {
  private final SchemaCollector fields;

  public interface SchemaCollector {
    void add(Field field);
    void addAll(Iterator<Field> fields);
    void addAll(Iterable<Field> fields);
    ImmutableCollection<Field> build();
  }

  public static SchemaLegacy empty() {
    return builder().build();
  }

  public static SchemaBuilder builder() {
    return new SchemaBuilder(new ListSchemaBuilder());
  }

  public static SchemaBuilder uniqueNameBuilder() {
    return new SchemaBuilder(new SetSchemaBuilder());
  }

  SchemaBuilder(SchemaCollector collector) {
    this.fields = collector;
  }

  public SchemaBuilder add(Field field) {
    fields.add(field);
    return this;
  }

  public SchemaBuilder add(QualifiedIdentifier id, Type type) {
    add(new Field(id, type));
    return this;
  }

  @Deprecated
  public SchemaBuilder add(String name, TypeDesc legacyType) {
    add(toQualifiedIdentifier(name), TypeConverter.convert(legacyType));
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
    return addAll2(columns, new Function<Column, Field>() {
      @Override
      public Field apply(@Nullable Column input) {
        return FieldConverter.convert(input);
      }
    });
  }

  @Deprecated
  public SchemaBuilder addAll(Column [] columns) {
    return addAll2(columns, new Function<Column, Field>() {
      @Override
      public Field apply(@Nullable Column input) {
        return FieldConverter.convert(input);
      }
    });
  }

  @Deprecated
  public <T> SchemaBuilder addAll(T [] fields, Function<T, Column> fn) {
    for (T t : fields) {
      add(fn.apply(t));
    }
    return this;
  }

  @Deprecated
  public <T> SchemaBuilder addAll(Iterable<T> fields, Function<T, Column> fn) {
    for (T t : fields) {
      add(fn.apply(t));
    }
    return this;
  }

  @Deprecated
  public <T> SchemaBuilder addAll(Iterator<T> fields, Function<T, Column> fn) {
    while(fields.hasNext()) {
      T t = fields.next();
      add(fn.apply(t));
    }
    return this;
  }

  public SchemaBuilder addAll2(Iterable<Field> fields) {
    this.fields.addAll(fields);
    return this;
  }

  public <T> SchemaBuilder addAll2(T [] fields, Function<T, Field> fn) {
    for (T t : fields) {
      add(fn.apply(t));
    }
    return this;
  }

  public <T> SchemaBuilder addAll2(Iterable<T> fields, Function<T, Field> fn) {
    for (T t : fields) {
      add(fn.apply(t));
    }
    return this;
  }

  public <T> SchemaBuilder addAll2(Iterator<T> fields, Function<T, Field> fn) {
    while(fields.hasNext()) {
      T t = fields.next();
      add(fn.apply(t));
    }
    return this;
  }

  @Deprecated
  public SchemaLegacy build() {
    ImmutableList.Builder<Column> columns = new ImmutableList.Builder();
    for (Field field : fields.build()) {
      columns.add(new Column(field.name().interned(), field.type()));
    }

    return new SchemaLegacy(columns.build());
  }

  public org.apache.tajo.schema.Schema buildV2() {
    return new org.apache.tajo.schema.Schema(fields.build());
  }
}
