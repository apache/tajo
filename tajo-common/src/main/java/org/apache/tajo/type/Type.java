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

package org.apache.tajo.type;

import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.TypeProto;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.schema.Field;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Represents Type
 */
public abstract class Type implements Cloneable, ProtoObject<TypeProto> {

  public static int DEFAULT_PRECISION = 0;
  public static int DEFAULT_SCALE = 0;

  // No paramter types
  public static final Any Any = new Any();
  public static final Bit Bit = new Bit();
  public static final Null Null = new Null();
  public static final Bool Bool = new Bool();
  public static final Int1 Int1 = new Int1();
  public static final Int2 Int2 = new Int2();
  public static final Int4 Int4 = new Int4();
  public static final Int8 Int8 = new Int8();
  public static final Float4 Float4 = new Float4();
  public static final Float8 Float8 = new Float8();
  public static final Numeric Numeric = new Numeric(DEFAULT_PRECISION, DEFAULT_SCALE);
  public static final Date Date = new Date();
  public static final Time Time = new Time();
  public static final Timestamp Timestamp = new Timestamp();
  public static final Interval Interval = new Interval();
  public static final Text Text = new Text();
  public static final Blob Blob = new Blob();

  protected TajoDataTypes.Type kind;

  public Type(TajoDataTypes.Type kind) {
    this.kind = kind;
  }

  public TajoDataTypes.Type kind() {
    return kind;
  }

  public boolean isTypeParameterized() {
    return false;
  }

  public boolean isValueParameterized() {
    return false;
  }

  public List<Type> getTypeParameters() {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  public List<Integer> getValueParameters() {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  protected static String typeName(TajoDataTypes.Type type) {
    return type.name().toUpperCase();
  }

  @Override
  public int hashCode() {
    return kind().hashCode();
  }

  @Override
  public boolean equals(Object t) {
    return t instanceof Type && ((Type)t).kind() == kind();
  }

  @Override
  public String toString() {
    return typeName(kind());
  }

  public boolean isAny() {
    return this.kind() == TajoDataTypes.Type.ANY;
  }

  public boolean isStruct() {
    return this.kind() == TajoDataTypes.Type.RECORD;
  }

  public boolean isNull() { return this.kind() == TajoDataTypes.Type.NULL_TYPE; }

  public static Numeric Numeric(int precision) {
    return new Numeric(precision, DEFAULT_SCALE);
  }

  public static Numeric Numeric(int precision, int scale) {
    return new Numeric(precision, scale);
  }

  public static Char Char(int len) {
    return new Char(len);
  }

  public static Varchar Varchar(int len) {
    return new Varchar(len);
  }

  public static Record Record(Collection<Field> types) {
    return new Record(types);
  }

  public static Array Array(Type type) {
    return new Array(type);
  }

  public static Record Record(Field... types) {
    return new Record(Arrays.asList(types));
  }

  public static Map Map(Type keyType, Type valueType) {
    return new Map(keyType, valueType);
  }

  public static Null Null() {
    return new Null();
  }

  @Override
  public TypeProto getProto() {
    return TypeProtobufEncoder.encode(this);
  }
}
