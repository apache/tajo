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

import com.google.common.primitives.Booleans;
import com.google.gson.annotations.Expose;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidOperationException;

public class BooleanDatum extends Datum {
  @Expose private final boolean val;
  public static final String TRUE_STRING ="t";
  public static final String FALSE_STRING ="f";
  public static final BooleanDatum TRUE = new BooleanDatum(true);
  public static final BooleanDatum FALSE = new BooleanDatum(false);

  // Three-valued Login Constants
  public static final byte UNKNOWN_INT = 0;
  public static final byte TRUE_INT = 1;
  public static final byte FALSE_INT = 2;
  public static final byte [] TRUE_BYTES = new byte[] {TRUE_INT};
  public static final byte [] FALSE_BYTES = new byte[] {FALSE_INT};

  /** 0 - UNKNOWN, 1 - TRUE, 2 - FALSE */
  public static final Datum [] THREE_VALUES = new Datum [] {
      NullDatum.get(), TRUE, FALSE
  };

  public static final Datum [][] AND_LOGIC = new Datum [][] {
      //               unknown       true            false
      new Datum [] {NullDatum.get(), NullDatum.get(), FALSE}, // unknown
      new Datum [] {NullDatum.get(), TRUE,            FALSE}, // true
      new Datum [] {FALSE,           FALSE,           FALSE}  // false
  };

  public static final Datum [][] OR_LOGIC = new Datum [][] {
      //               unknown       true       false
      new Datum [] {NullDatum.get(), TRUE, NullDatum.get()}, // unknown
      new Datum [] {TRUE,            TRUE, TRUE           }, // true
      new Datum [] {NullDatum.get(), TRUE, FALSE          }  // false
  };

  private BooleanDatum(boolean val) {
    super(TajoDataTypes.Type.BOOLEAN);
    this.val = val;
  }

  protected BooleanDatum(byte byteVal) {
    super(TajoDataTypes.Type.BOOLEAN);
    this.val = byteVal == TRUE_INT;
  }

  protected BooleanDatum(int byteVal) {
    super(TajoDataTypes.Type.BOOLEAN);
    this.val = byteVal == TRUE_INT;
  }


  protected BooleanDatum(byte[] bytes) {
    this(bytes[0]); // get the first byte
  }

  public boolean asBool() {
    return val;
  }

  @Override
  public char asChar() {
    return val ? 't' : 'f';
  }

  @Override
  public short asInt2() {
    return (short) (val ? TRUE_INT : FALSE_INT);
  }

  @Override
  public int asInt4() {
    return val ? TRUE_INT : FALSE_INT;
  }

  @Override
  public long asInt8() {
    return val ? TRUE_INT : FALSE_INT;
  }

  @Override
  public byte asByte() {
    return (byte) (val ? TRUE_INT : FALSE_INT);
  }

  @Override
  public byte[] asByteArray() {
    return val ? TRUE_BYTES : FALSE_BYTES;
  }

  @Override
  public float asFloat4() {
    return val ? TRUE_INT : FALSE_INT;
  }

  @Override
  public double asFloat8() {
    return val ? TRUE_INT : FALSE_INT;
  }

  @Override
  public String asChars() {
    return val ? TRUE_STRING : FALSE_STRING;
  }

  @Override
  public Datum and(Datum datum) {
    return AND_LOGIC[asInt4()][datum.asInt4()];
  }

  @Override
  public Datum or(Datum datum) {
    return OR_LOGIC[asInt4()][datum.asInt4()];
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public int hashCode() {
    return val ? 7907 : 0; // 7907 is one of the prime numbers
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BooleanDatum) {
      BooleanDatum other = (BooleanDatum) obj;
      return val == other.val;
    }

    return false;
  }

  // Datum Comparator
  public BooleanDatum equalsTo(Datum datum) {
    switch(datum.type()) {
    case BOOLEAN: return DatumFactory.createBool(this.val == ((BooleanDatum)datum).val);
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case BOOLEAN:
      return Booleans.compare(val, datum.asBool());
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
}
