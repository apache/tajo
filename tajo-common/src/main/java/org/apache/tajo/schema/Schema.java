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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.type.Type;
import org.apache.tajo.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

import static org.apache.tajo.common.TajoDataTypes.Type.RECORD;

/**
 * A field is a pair of a name and a type. Schema is an ordered list of fields.
 */
public class Schema implements Iterable<Schema.NamedType> {
  private final ImmutableList<NamedType> namedTypes;

  public Schema(Collection<NamedType> namedTypes) {
    this.namedTypes = ImmutableList.copyOf(namedTypes);
  }

  public static Schema Schema(NamedType...fields) {
    return new Schema(Arrays.asList(fields));
  }

  public static Schema Schema(Collection<NamedType> fields) {
    return new Schema(fields);
  }

  @Override
  public String toString() {
    return StringUtils.join(namedTypes, ",");
  }

  public static NamedStructType Struct(QualifiedIdentifier name, NamedType... namedTypes) {
    return Struct(name, Arrays.asList(namedTypes));
  }

  public static NamedStructType Struct(QualifiedIdentifier name, Collection<NamedType> namedTypes) {
    return new NamedStructType(name, namedTypes);
  }

  public static NamedPrimitiveType Field(QualifiedIdentifier name, Type type) {
    return new NamedPrimitiveType(name, type);
  }

  @Override
  public Iterator<NamedType> iterator() {
    return namedTypes.iterator();
  }

  public static abstract class NamedType {
    protected final QualifiedIdentifier name;

    public NamedType(QualifiedIdentifier name) {
      this.name = name;
    }

    public QualifiedIdentifier name() {
      return this.name;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {

      if (this == obj) {
        return true;
      }

      if (obj instanceof NamedType) {
        NamedType other = (NamedType) obj;
        return this.name.equals(other.name);
      }

      return false;
    }
  }

  public static class NamedPrimitiveType extends NamedType {
    private final Type type;

    public NamedPrimitiveType(QualifiedIdentifier name, Type type) {
      super(name);
      Preconditions.checkArgument(type.baseType() != RECORD);
      this.type = type;
    }

    public Type type() {
      return type;
    }

    @Override
    public String toString() {
      return name + " " + type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (obj instanceof NamedPrimitiveType) {
        NamedPrimitiveType other = (NamedPrimitiveType) obj;
        return super.equals(other) && this.type.equals(type);
      }

      return false;
    }
  }

  public static class NamedStructType extends NamedType {
    private final ImmutableList<NamedType> fields;

    public NamedStructType(QualifiedIdentifier name, Collection<NamedType> fields) {
      super(name);
      this.fields = ImmutableList.copyOf(fields);
    }

    public Collection<NamedType> fields() {
      return this.fields;
    }

    @Override
    public String toString() {
      return name + " record (" + StringUtils.join(fields, ",") + ")";
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, fields);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (obj instanceof NamedStructType) {
        NamedStructType other = (NamedStructType) obj;
        return super.equals(other) && fields.equals(other.fields);
      }

      return false;
    }
  }
}
