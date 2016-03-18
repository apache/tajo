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
  public abstract TajoDataTypes.Type baseType();

  protected static String typeName(TajoDataTypes.Type type) {
    return type.name().toLowerCase();
  }

  @Override
  public int hashCode() {
    return baseType().hashCode();
  }

  @Override
  public boolean equals(Object t) {
    if (t instanceof Type) {
      return ((Type)t).baseType() == baseType();
    }
    return false;
  }

  @Override
  public String toString() {
    return typeName(baseType());
  }

  public static Bool Bool() {
    return new Bool();
  }

  public static Int2 Int2() {
    return new Int2();
  }

  public static Int4 Int4() {
    return new Int4();
  }

  public static Int8 Int8() {
    return new Int8();
  }

  public static Float4 Float4() {
    return new Float4();
  }

  public static Float8 Float8() {
    return new Float8();
  }

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
}
