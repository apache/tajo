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

package org.apache.tajo.datum;

import org.apache.tajo.util.Bytes;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class NullDatum extends Datum {
  private static final NullDatum instance;
  
  static {
    instance = new NullDatum();
  }
  
  private NullDatum() {
    super(Type.ANY);
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
  public short asInt2() {
    return Short.MIN_VALUE;
  }

  @Override
  public int asInt4() {
    return Integer.MIN_VALUE;
  }

  @Override
  public long asInt8() {
    return Long.MIN_VALUE;
  }

  @Override
  public byte[] asByteArray() {
    return Bytes.toBytes("NULL");
  }

  @Override
  public float asFloat4() {
    return Float.NaN;
  }

  @Override
  public double asFloat8() {
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
    return obj instanceof NullDatum;
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