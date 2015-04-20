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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.MurmurHash;
import org.apache.tajo.util.NumberUtil;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.nio.ByteBuffer;


public class Float8Datum extends NumericDatum {
  private static final int size = 8;
  @Expose private final double val;

	public Float8Datum(double val) {
    super(TajoDataTypes.Type.FLOAT8);
		this.val = val;
	}

  public Float8Datum(byte[] bytes) {
    super(TajoDataTypes.Type.FLOAT8);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    this.val = bb.getDouble();
  }

  @Override
  public char asChar() {
    return asChars().charAt(0);
  }

	@Override
	public short asInt2() {
		return (short) val;
	}

	@Override
	public int asInt4() {
		return (int) val;
	}

  @Override
	public long asInt8() {
		return (long) val;
	}

  @Override
	public byte[] asByteArray() {
		return Bytes.toBytes(val);
	}

  @Override
	public float asFloat4() {
		return (float) val;
	}

  @Override
	public double asFloat8() {
		return val;
	}

  @Override
	public String asChars() {
		return ""+val;
	}

  @Override
  public byte[] asTextBytes() {
    return NumberUtil.toAsciiBytes(val);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int hashCode() {
    return MurmurHash.hash(val);
  }

  public boolean equals(Object obj) {
    if (obj instanceof Float8Datum) {
      Float8Datum other = (Float8Datum) obj;
      return val == other.val;
    }

    return false;
  }

  @Override
  public Datum equalsTo(Datum datum) {
    switch (datum.type()) {
    case INT2:
      return DatumFactory.createBool(val == datum.asInt2());
    case INT4:
      return DatumFactory.createBool(val == datum.asInt4());
    case INT8:
      return DatumFactory.createBool(val == datum.asInt8());
    case FLOAT4:
      return DatumFactory.createBool(val == datum.asFloat4());
    case FLOAT8:
      return DatumFactory.createBool(val == datum.asFloat8());
    case NULL_TYPE:
      return datum;
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
      case INT2: {
        short another = datum.asInt2();
        if (val < another) {
          return -1;
        } else if (val > another) {
          return 1;
        } else {
          return 0;
        }
      }
      case INT4: {
        int another = datum.asInt4();
        if (val < another) {
          return -1;
        } else if (val > another) {
          return 1;
        } else {
          return 0;
        }
      }
      case INT8:{
        long another = datum.asInt8();
        if (val < another) {
          return -1;
        } else if (val > another) {
          return 1;
        } else {
          return 0;
        }
      }
      case FLOAT4: {
        float another = datum.asFloat4();
        if (val < another) {
          return -1;
        } else if (val > another) {
          return 1;
        } else {
          return 0;
        }
      }
      case FLOAT8: {
        double another = datum.asFloat8();
        if (val < another) {
          return -1;
        } else if (val > another) {
          return 1;
        } else {
          return 0;
        }
      }
      case NULL_TYPE:
        return -1;
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum plus(Datum datum) {
    switch (datum.type()) {
    case INT2:
      return DatumFactory.createFloat8(val + datum.asInt2());
    case INT4:
      return DatumFactory.createFloat8(val + datum.asInt4());
    case INT8:
      return DatumFactory.createFloat8(val + datum.asInt8());
    case FLOAT4:
      return DatumFactory.createFloat8(val + datum.asFloat4());
    case FLOAT8:
      return DatumFactory.createFloat8(val + datum.asFloat8());
    case DATE:
      DateDatum dateDatum = (DateDatum)datum;
      TimeMeta tm = dateDatum.toTimeMeta();
      tm.plusDays(asInt4());
      return new DateDatum(tm);
    case NULL_TYPE:
      return datum;
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum minus(Datum datum) {
    switch (datum.type()) {
    case INT2:
      return DatumFactory.createFloat8(val - datum.asInt2());
    case INT4:
      return DatumFactory.createFloat8(val - datum.asInt4());
    case INT8:
      return DatumFactory.createFloat8(val - datum.asInt8());
    case FLOAT4:
      return DatumFactory.createFloat8(val - datum.asFloat4());
    case FLOAT8:
      return DatumFactory.createFloat8(val - datum.asFloat8());
    case DATE:
      DateDatum dateDatum = (DateDatum)datum;
      TimeMeta tm = dateDatum.toTimeMeta();
      tm.plusDays(0 - asInt4());
      return new DateDatum(tm);
    case NULL_TYPE:
      return datum;
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum multiply(Datum datum) {
    switch (datum.type()) {
    case INT2:
      return DatumFactory.createFloat8(val * datum.asInt2());
    case INT4:
      return DatumFactory.createFloat8(val * datum.asInt4());
    case INT8:
      return DatumFactory.createFloat8(val * datum.asInt8());
    case FLOAT4:
      return DatumFactory.createFloat8(val * datum.asFloat4());
    case FLOAT8:
      return DatumFactory.createFloat8(val * datum.asFloat8());
    case INTERVAL:
      IntervalDatum interval = (IntervalDatum)datum;
      return new IntervalDatum((int)(interval.getMonths() * val), (long)(interval.getMilliSeconds() * val));
    case NULL_TYPE:
      return datum;
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum divide(Datum datum) {
    switch (datum.type()) {
      case INT2:
        short paramValueI2 = datum.asInt2();
        if (!validateDivideZero(paramValueI2)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val / paramValueI2);
      case INT4:
        int paramValueI4 = datum.asInt4();
        if (!validateDivideZero(paramValueI4)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val / paramValueI4);
      case INT8:
        long paramValueI8 = datum.asInt8();
        if (!validateDivideZero(paramValueI8)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val / paramValueI8);
      case FLOAT4:
        float paramValueF4 = datum.asFloat4();
        if (!validateDivideZero(paramValueF4)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val / paramValueF4);
      case FLOAT8:
        double paramValueF8 = datum.asFloat8();
        if (!validateDivideZero(paramValueF8)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val / paramValueF8);
      case NULL_TYPE:
        return datum;
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum modular(Datum datum) {
    switch (datum.type()) {
      case INT2:
        short paramValueI2 = datum.asInt2();
        if (!validateDivideZero(paramValueI2)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val % paramValueI2);
      case INT4:
        int paramValueI4 = datum.asInt4();
        if (!validateDivideZero(paramValueI4)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val % paramValueI4);
      case INT8:
        long paramValueI8 = datum.asInt8();
        if (!validateDivideZero(paramValueI8)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val % paramValueI8);
      case FLOAT4:
        float paramValueF4 = datum.asFloat4();
        if (!validateDivideZero(paramValueF4)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val % paramValueF4);
      case FLOAT8:
        double paramValueF8 = datum.asFloat8();
        if (!validateDivideZero(paramValueF8)) {
          return NullDatum.get();
        }
        return DatumFactory.createFloat8(val % paramValueF8);
      case NULL_TYPE:
        return datum;
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public NumericDatum inverseSign() {
    return new Float8Datum(-val);
  }
}
