/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.datum;

import tajo.util.Bytes;

public class NullDatum extends Datum {
  private static final NullDatum instance;
  
  static {
    instance = new NullDatum();
  }
  
  private NullDatum() {
    super(DatumType.NULL);
  }
  
  public static NullDatum get() {
    return instance;
  }
  
  @Override
  public boolean asBool() {
    return false;
  }

  @Override
  public byte asByte() {
    return 0;
  }

  @Override
  public short asShort() {
    return Short.MIN_VALUE;
  }

  @Override
  public int asInt() {
    return Integer.MIN_VALUE;
  }

  @Override
  public long asLong() {
    return Long.MIN_VALUE;
  }

  @Override
  public byte[] asByteArray() {
    return Bytes.toBytes("NULL");
  }

  @Override
  public float asFloat() {
    return Float.NaN;
  }

  @Override
  public double asDouble() {
    return Double.NaN;
  }

  @Override
  public String asChars() {
    return "NULL";
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NullDatum) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(Datum datum) {
    return 0;
  }

  @Override
  public int hashCode() {
    return 23244; // one of the prime number
  }

  @Override
  public String toJSON() {
    return "";
  }
}