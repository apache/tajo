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
import org.apache.tajo.SessionVars;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.type.TajoTypeUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.exception.InvalidValueForCastException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.json.CommonGsonHelper;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.datetime.TimeMeta;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

public abstract class Datum implements Comparable<Datum>, GsonObject {
  static boolean abortWhenDivideByZero;

  static {
    try {
      //TODO separate hadoop configuration from TajoConf
      initAbortWhenDivideByZero(new TajoConf());
    } catch (NoClassDefFoundError error) {
      abortWhenDivideByZero = Boolean.valueOf(System.getProperty(SessionVars.ARITHABORT.getConfVars().keyname()
          , SessionVars.ARITHABORT.getConfVars().defaultVal));
    }
  }

  @Expose	protected final org.apache.tajo.type.Type type;

  public Datum(org.apache.tajo.type.Type type) {
    this.type = type;
  }

  public org.apache.tajo.type.Type type() {
    return this.type;
  }

  public TajoDataTypes.Type kind() {
    return this.type.kind();
  }

  public boolean isTrue() {
    return type.kind() == BOOLEAN && asBool();
  }

  public boolean isNull() {
    return false;
  }

  public boolean isNotNull() {
    return true;
  }

  public boolean asBool() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type.kind(), BOOLEAN));
  }

  public byte asByte() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, BIT));
  }

  public char asChar() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, CHAR));
  }

  public short asInt2() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, INT2));
  }

  public int asInt4() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, INT4));
  }

  public long asInt8() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, INT8));
  }

  public byte [] asByteArray() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, BLOB));
  }

  public float asFloat4() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, FLOAT4));
  }

  public double asFloat8() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, FLOAT8));
  }

  public String asChars() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, TEXT));
  }

  // todo remove this
  public char [] asUnicodeChars() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, TEXT));
  }

  public byte[] asTextBytes() {
    return asChars().getBytes(TextDatum.DEFAULT_CHARSET);
  }

  public TimeMeta asTimeMeta() {
    throw new TajoRuntimeException(new InvalidValueForCastException(type, TajoDataTypes.Type.INT8));
  }

  public boolean isNumeric() {
    return TajoTypeUtil.isNumeric(type);
  }

  public boolean isNumber() {
    return TajoTypeUtil.isNumber(type);
  }

  public boolean isReal() {
    return TajoTypeUtil.isReal(type.kind());
  }

  protected static void initAbortWhenDivideByZero(TajoConf tajoConf) {
    abortWhenDivideByZero = tajoConf.getBoolVar(ConfVars.$BEHAVIOR_ARITHMETIC_ABORT);
  }

  public abstract int size();

  // belows should be extracted out of datum
  public Datum and(Datum datum) {
    throw new InvalidOperationException(type);
  }

  public Datum or(Datum datum) {
    throw new InvalidOperationException(type);
  }

  public Datum plus(Datum datum) {
    throw new InvalidOperationException(type);
  }

  public Datum minus(Datum datum) {
    throw new InvalidOperationException(type);
  }

  public Datum multiply(Datum datum) {
    throw new InvalidOperationException(type);
  }

  public Datum divide(Datum datum) {
    throw new InvalidOperationException(type);
  }

  public Datum modular(Datum datum) {
    throw new InvalidOperationException(type);
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
    if (type.isNull() || datum.type().isNull()) {
      return NullDatum.get();
    }
    return DatumFactory.createBool(compareTo(datum) < 0);
  }

  public Datum lessThanEqual(Datum datum) {
    if (type.isNull() || datum.type().isNull()) {
      return NullDatum.get();
    }
    return DatumFactory.createBool(compareTo(datum) <= 0);
  }

  public Datum greaterThan(Datum datum) {
    if (type.isNull() || datum.type().isNull()) {
      return NullDatum.get();
    }
    return DatumFactory.createBool(compareTo(datum) > 0);
  }

  public Datum greaterThanEqual(Datum datum) {
    if (type.isNull() || datum.type().isNull()) {
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