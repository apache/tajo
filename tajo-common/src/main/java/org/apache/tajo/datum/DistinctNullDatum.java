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

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.util.MurmurHash;

public class DistinctNullDatum extends Datum {
  private static DistinctNullDatum instance;
  private static final byte [] EMPTY_BYTES = new byte[0];
  private static final TajoDataTypes.DataType DISTINCT_NULL_DATA_TYPE;
  private static final String EMPTY_STRING = "\\NN";

  static {
    instance = new DistinctNullDatum();
    DISTINCT_NULL_DATA_TYPE = TajoDataTypes.DataType.newBuilder().setType(TajoDataTypes.Type.DISTINCT_NULL_TYPE).build();
  }

  private DistinctNullDatum() {
    super(TajoDataTypes.Type.DISTINCT_NULL_TYPE);
  }

  public static DistinctNullDatum get() {
    return instance;
  }

  public static TajoDataTypes.DataType getDataType() {
    return DISTINCT_NULL_DATA_TYPE;
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public boolean asBool() {
    throw new InvalidCastException(TajoDataTypes.Type.DISTINCT_NULL_TYPE, TajoDataTypes.Type.BOOLEAN);
  }

  @Override
  public byte asByte() {
    return 0;
  }

  @Override
  public short asInt2() {
    return 0;
  }

  @Override
  public int asInt4() {
    return 0;
  }

  @Override
  public long asInt8() {
    return 0;
  }

  @Override
  public byte[] asByteArray() {
    return EMPTY_BYTES;
  }

  @Override
  public float asFloat4() {
    return 0f;
  }

  @Override
  public double asFloat8() {
    return 0d;
  }

  @Override
  public String asChars() {
    return "";
  }

  @Override
  public byte[] asTextBytes() {
    return EMPTY_BYTES;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof DistinctNullDatum;
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.DISTINCT_NULL_TYPE) {
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public Datum and(Datum datum) {
    return BooleanDatum.AND_LOGIC[BooleanDatum.UNKNOWN_INT][datum.asInt4()];
  }

  @Override
  public Datum or(Datum datum) {
    return BooleanDatum.OR_LOGIC[BooleanDatum.UNKNOWN_INT][datum.asInt4()];
  }

  public DistinctNullDatum plus(Datum datum) {
    return this;
  }

  public DistinctNullDatum minus(Datum datum) {
    return this;
  }

  public DistinctNullDatum multiply(Datum datum) {
    return this;
  }

  public DistinctNullDatum divide(Datum datum) {
    return this;
  }

  public DistinctNullDatum modular(Datum datum) {
    return this;
  }

  public DistinctNullDatum equalsTo(Datum datum) {
    return this;
  }

  public DistinctNullDatum lessThan(Datum datum) {
    return this;
  }

  public DistinctNullDatum lessThanEqual(Datum datum) {
    return this;
  }

  public DistinctNullDatum greaterThan(Datum datum) {
    return this;
  }

  public DistinctNullDatum greaterThanEqual(Datum datum) {
    return this;
  }

  @Override
  public int hashCode() {
    return MurmurHash.hash(EMPTY_STRING.getBytes());
  }

  @Override
  public String toString() {
    return "DISTINCT_NULL";
  }
}
