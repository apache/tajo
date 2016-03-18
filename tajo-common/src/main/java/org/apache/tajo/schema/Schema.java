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

import static org.apache.tajo.common.TajoDataTypes.Type.RECORD;

/**
 * A field is a pair of a name and a type. Schema is an ordered list of fields.
 */
public class Schema {
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

  public static NamedStructType Struct(String name, NamedType... namedTypes) {
    return new NamedStructType(name, Arrays.asList(namedTypes));
  }

  public static NamedStructType Struct(String name, Collection<NamedType> namedTypes) {
    return new NamedStructType(name, namedTypes);
  }

  public static NamedPrimitiveType Field(String name, Type type) {
    return new NamedPrimitiveType(name, type);
  }

  public static abstract class NamedType {
    protected final String name;

    public NamedType(String name) {
      this.name = name;
    }

    public String name() {
      return this.name;
    }
  }

  public static class NamedPrimitiveType extends NamedType {
    private final Type type;

    NamedPrimitiveType(String name, Type type) {
      super(name);
      Preconditions.checkArgument(type.baseType() != RECORD);
      this.type = type;
    }

    @Override
    public String toString() {
      return name + " " + type;
    }
  }

  public static class NamedStructType extends NamedType {
    private final ImmutableList<NamedType> namedTypes;

    public NamedStructType(String name, Collection<NamedType> namedTypes) {
      super(name);
      this.namedTypes = ImmutableList.copyOf(namedTypes);
    }

    @Override
    public String toString() {
      return name + " record (" + StringUtils.join(namedTypes, ",") + ")";
    }
  }
}
