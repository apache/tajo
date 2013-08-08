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

/**
 * 
 */
package org.apache.tajo.datum;

import com.google.gson.annotations.Expose;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.exception.InvalidOperationException;

import java.nio.ByteBuffer;

public class Float8Datum extends NumericDatum {
  private static final int size = 8;
  @Expose private double val;

	public Float8Datum() {
		super(TajoDataTypes.Type.FLOAT8);
	}
	
	public Float8Datum(double val) {
		this();
		this.val = val;
	}

  public Float8Datum(byte[] bytes) {
    this();
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    this.val = bb.getDouble();
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
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(val);
		return bb.array();
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
  public int size() {
    return size;
  }
  
  @Override
  public int hashCode() {
    return (int) val;
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof Float8Datum) {
      Float8Datum other = (Float8Datum) obj;
      return val == other.val;
    }
    
    return false;
  }

  @Override
  public BooleanDatum equalsTo(Datum datum) {
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
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
      case INT2:
        if (val < datum.asInt2()) {
          return -1;
        } else if (datum.asInt2() < val) {
          return 1;
        } else {
          return 0;
        }
      case INT4:
        if (val < datum.asInt4()) {
          return -1;
        } else if (datum.asInt4() < val) {
          return 1;
        } else {
          return 0;
        }
      case INT8:
        if (val < datum.asInt8()) {
          return -1;
        } else if (datum.asInt8() < val) {
          return 1;
        } else {
          return 0;
        }
      case FLOAT4:
        if (val < datum.asFloat4()) {
          return -1;
        } else if (datum.asFloat4() < val) {
          return 1;
        } else {
          return 0;
        }
      case FLOAT8:
        if (val < datum.asFloat8()) {
          return -1;
        } else if (datum.asFloat8() < val) {
          return 1;
        } else {
          return 0;
        }
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
    default:
      throw new InvalidOperationException();
    }
  }

  @Override
  public Datum divide(Datum datum) {
    switch (datum.type()) {
    case INT2:
      return DatumFactory.createFloat8(val / datum.asInt2());
    case INT4:
      return DatumFactory.createFloat8(val / datum.asInt4());
    case INT8:
      return DatumFactory.createFloat8(val / datum.asInt8());
    case FLOAT4:
      return DatumFactory.createFloat8(val / datum.asFloat4());
    case FLOAT8:
      return DatumFactory.createFloat8(val / datum.asFloat8());
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public Datum modular(Datum datum) {
    switch (datum.type()) {
      case INT2:
        return DatumFactory.createFloat8(val % datum.asInt2());
      case INT4:
        return DatumFactory.createFloat8(val % datum.asInt4());
      case INT8:
        return DatumFactory.createFloat8(val % datum.asInt8());
      case FLOAT4:
        return DatumFactory.createFloat8(val % datum.asFloat4());
      case FLOAT8:
        return DatumFactory.createFloat8(val % datum.asFloat8());
      default:
        throw new InvalidOperationException(datum.type());
    }
  }
  
  @Override
  public void inverseSign() {   
    this.val = -val;
  }
}
