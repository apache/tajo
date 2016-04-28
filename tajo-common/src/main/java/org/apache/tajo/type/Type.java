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

import org.apache.tajo.common.TajoDataTypes;

import java.util.Arrays;
import java.util.Collection;

public abstract class Type {

  // No paramter types
  public static final Any Any = new Any();
  public static final Null Null = new Null();
  public static final Bool Bool = new Bool();
  public static final Int1 Int1 = new Int1();
  public static final Int2 Int2 = new Int2();
  public static final Int4 Int4 = new Int4();
  public static final Int8 Int8 = new Int8();
  public static final Float4 Float4 = new Float4();
  public static final Float8 Float8 = new Float8();
  public static final Date Date = new Date();
  public static final Time Time = new Time();
  public static final Timestamp Timestamp = new Timestamp();
  public static final Interval Interval = new Interval();
  public static final Text Text = new Text();
  public static final Blob Blob = new Blob();
  public static final Inet4 Inet4 = new Inet4();

  public abstract TajoDataTypes.Type baseType();

  public boolean hasParam() {
    return false;
  }

  protected static String typeName(TajoDataTypes.Type type) {
    return type.name().toLowerCase();
  }

  @Override
  public int hashCode() {
    return baseType().hashCode();
  }

  @Override
  public boolean equals(Object t) {
    return t instanceof Type && ((Type)t).baseType() == baseType();
  }

  @Override
  public String toString() {
    return typeName(baseType());
  }

  public boolean isStruct() {
    return this.baseType() == TajoDataTypes.Type.RECORD;
  }

  public boolean isNull() { return this.baseType() == TajoDataTypes.Type.NULL_TYPE; }

  public static int DEFAULT_SCALE = 0;

  public static Numeric Numeric(int precision) {
    return new Numeric(precision, DEFAULT_SCALE);
  }

  public static Numeric Numeric(int precision, int scale) {
    return new Numeric(precision, scale);
  }

  public static Date Date() {
    return new Date();
  }

  public static Time Time() {
    return new Time();
  }

  public static Timestamp Timestamp() {
    return new Timestamp();
  }

  public static Interval Interval() {
    return new Interval();
  }

  public static Char Char(int len) {
    return new Char(len);
  }

  public static Varchar Varchar(int len) {
    return new Varchar(len);
  }

  public static Text Text() {
    return new Text();
  }

  public static Blob Blob() {
    return new Blob();
  }

  public static Inet4 Inet4() {
    return new Inet4();
  }

  public static Struct Struct(Collection<Type> types) {
    return new Struct(types);
  }

  public static Struct Struct(Type ... types) {
    return new Struct(Arrays.asList(types));
  }

  public static Array Array(Type type) {
    return new Array(type);
  }

  public static Map Map(Type keyType, Type valueType) {
    return new Map(keyType, valueType);
  }

  public static Null Null() {
    return new Null();
  }
}
