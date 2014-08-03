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

import com.google.gson.annotations.Expose;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.json.CommonGsonHelper;
import org.apache.tajo.json.GsonObject;

import static org.apache.tajo.common.TajoDataTypes.Type;

public abstract class Datum implements Comparable<Datum>, GsonObject {
  static boolean abortWhenDivideByZero;

  static {
    initAbortWhenDivideByZero(new TajoConf());
  }

  @Expose	private final Type type;

  public Datum(Type type) {
    this.type = type;
  }

  public Type type() {
    return this.type;
  }

  public boolean isTrue() {
    return type == Type.BOOLEAN && asBool();
  }

  public boolean isNull() {
    return false;
  }

  public boolean asBool() {
    throw new InvalidCastException(type, Type.BOOLEAN);
  }

  public byte asByte() {
    throw new InvalidCastException(type, Type.BIT);
  }

  public char asChar() {
    throw new InvalidCastException(type, Type.CHAR);
  }

  public short asInt2() {
    throw new InvalidCastException(type, Type.INT2);
  }
  public int asInt4() {
    throw new InvalidCastException(type, Type.INT1);
  }

  public long asInt8() {
    throw new InvalidCastException(type, Type.INT8);
  }

  public byte [] asByteArray() {
    throw new InvalidCastException(type, Type.BLOB);
  }

  public float asFloat4() {
    throw new InvalidCastException(type, Type.FLOAT4);
  }

  public double asFloat8() {
    throw new InvalidCastException(type, Type.FLOAT8);
  }

  public String asChars() {
    throw new InvalidCastException(type, Type.TEXT);
  }

  public byte[] asTextBytes() {
    return toString().getBytes();
  }

  public boolean isNumeric() {
    return isNumber() || isReal();
  }

  public boolean isNumber() {
    return
        this.type == Type.INT2 ||
            this.type == Type.INT4 ||
            this.type == Type.INT8;
  }

  public boolean isReal() {
    return
        this.type == Type.FLOAT4||
            this.type == Type.FLOAT8;
  }

  protected static void initAbortWhenDivideByZero(TajoConf tajoConf) {
    abortWhenDivideByZero = tajoConf.getBoolVar(ConfVars.BEHAVIOR_ARITHMETIC_ABORT);
  }

  public abstract int size();

  public Datum and(Datum datum) {
    throw new InvalidOperationException(datum.type);
  }

  public Datum or(Datum datum) {
    throw new InvalidOperationException(datum.type);
  }

  public Datum plus(Datum datum) {
    throw new InvalidOperationException(datum.type);
  }

  public Datum minus(Datum datum) {
    throw new InvalidOperationException(datum.type);
  }

  public Datum multiply(Datum datum) {
    throw new InvalidOperationException(datum.type);
  }

  public Datum divide(Datum datum) {
    throw new InvalidOperationException(datum.type);
  }

  public Datum modular(Datum datum) {
    throw new InvalidOperationException(datum.type);
  }

  public Datum equalsTo(Datum datum) {
    if (this instanceof NullDatum || datum instanceof NullDatum) {
      return NullDatum.get();
    } else {
      return DatumFactory.createBool(compareTo(datum) == 0);
    }
  }

  public Datum notEqualsTo(Datum datum) {
    if (this instanceof NullDatum || datum instanceof NullDatum) {
      return NullDatum.get();
    } else {
      return DatumFactory.createBool(compareTo(datum) != 0);
    }
  }

  public Datum lessThan(Datum datum) {
    if (this.type() == Type.NULL_TYPE || datum.type() == Type.NULL_TYPE) {
      return NullDatum.get();
    }
    return DatumFactory.createBool(compareTo(datum) < 0);
  }

  public Datum lessThanEqual(Datum datum) {
    if (this.type() == Type.NULL_TYPE || datum.type() == Type.NULL_TYPE) {
      return NullDatum.get();
    }
    return DatumFactory.createBool(compareTo(datum) <= 0);
  }

  public Datum greaterThan(Datum datum) {
    if (this.type() == Type.NULL_TYPE || datum.type() == Type.NULL_TYPE) {
      return NullDatum.get();
    }
    return DatumFactory.createBool(compareTo(datum) > 0);
  }

  public Datum greaterThanEqual(Datum datum) {
    if (this.type() == Type.NULL_TYPE || datum.type() == Type.NULL_TYPE) {
      return NullDatum.get();
    }
    return DatumFactory.createBool(compareTo(datum) >= 0);
  }

  public abstract int compareTo(Datum datum);

  @Override
  public String toJson() {
    return CommonGsonHelper.toJson(this, Datum.class);
  }

  @Override
  public String toString() {
    return asChars();
  }

  protected boolean validateDivideZero(short value) {
    if (value == (short)0) {
      if (abortWhenDivideByZero) {
        throw new ArithmeticException("/ by zero");
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  protected boolean validateDivideZero(int value) {
    if (value == 0) {
      if (abortWhenDivideByZero) {
        throw new ArithmeticException("/ by zero");
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  protected boolean validateDivideZero(long value) {
    if (value == 0L) {
      if (abortWhenDivideByZero) {
        throw new ArithmeticException("/ by zero");
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  protected boolean validateDivideZero(float value) {
    if (value == 0.0f) {
      if (abortWhenDivideByZero) {
        throw new ArithmeticException("/ by zero");
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  protected boolean validateDivideZero(double value) {
    if (value == 0.0) {
      if (abortWhenDivideByZero) {
        throw new ArithmeticException("/ by zero");
      } else {
        return false;
      }
    } else {
      return true;
    }
  }
}