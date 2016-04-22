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

package org.apache.tajo.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.type.Type;
import org.apache.tajo.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 * A field is a pair of a name and a type. Schema is an ordered list of fields.
 */
public class Schema implements Iterable<Schema.Field> {
  private final ImmutableList<Field> fields;

  public Schema(Collection<Field> fields) {
    this.fields = ImmutableList.copyOf(fields);
  }

  public static Schema Schema(Field...fields) {
    return new Schema(Arrays.asList(fields));
  }

  public static Schema Schema(Collection<Field> fields) {
    return new Schema(fields);
  }

  @Override
  public String toString() {
    return StringUtils.join(fields, ",");
  }

  public static Field StructField(QualifiedIdentifier name, Field ... fields) {
    return StructField(name, Arrays.asList(fields));
  }

  public static Field StructField(QualifiedIdentifier name, Collection<Field> fields) {
    return new Field(Type.Struct(fields), name);
  }

  @VisibleForTesting
  public static Field Field(String name, Type type) {
    return new Field(type, QualifiedIdentifier.$(name));
  }

  public static Field Field(QualifiedIdentifier name, Type type) {
    return new Field(type, name);
  }

  @Override
  public Iterator<Field> iterator() {
    return fields.iterator();
  }

  public static class Field {
    protected final Type type;
    protected final QualifiedIdentifier name;

    public Field(Type type, QualifiedIdentifier name) {
      this.type = type;
      this.name = name;
    }

    public QualifiedIdentifier name() {
      return this.name;
    }

    public TajoDataTypes.Type baseType() {
      return this.type.baseType();
    }

    public <T extends Type> T type() {
      return (T) type;
    }

    public boolean isStruct() {
      return type.isStruct();
    }

    public boolean isNull() {
      return type.isNull();
    }

    @Override
    public String toString() {
      return name + " " + type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, name);
    }

    @Override
    public boolean equals(Object obj) {

      if (this == obj) {
        return true;
      }

      if (obj instanceof Field) {
        Field other = (Field) obj;
        return this.type.equals(other) && this.name.equals(other.name);
      }

      return false;
    }
  }
}
