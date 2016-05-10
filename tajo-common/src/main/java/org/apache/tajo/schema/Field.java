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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.type.Type;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * Represent a field in a schema.
 */
public class Field implements Cloneable {
  protected final QualifiedIdentifier name;
  protected final Type type;

  public Field(QualifiedIdentifier name, Type type) {
    this.type = type;
    this.name = name;
  }

  public static Field Record(QualifiedIdentifier name, Field ... fields) {
    return Record(name, Arrays.asList(fields));
  }

  public static Field Record(QualifiedIdentifier name, Collection<Field> fields) {
    return new Field(name, Type.Record(fields));
  }

  @VisibleForTesting
  public static Field Field(String name, Type type) {
    return new Field(QualifiedIdentifier.$(name), type);
  }

  public static Field Field(QualifiedIdentifier name, Type type) {
    return new Field(name, type);
  }

  public QualifiedIdentifier name() {
    return this.name;
  }

  public TajoDataTypes.Type baseType() {
    return this.type.kind();
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
    return name + " (" + type + ")";
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
      return type.equals(other.type) && name.equals(other.name);
    }

    return false;
  }

  @Override
  public Field clone() throws CloneNotSupportedException {
    return this;
  }
}
