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
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.datum.exception.InvalidCastException;
import org.apache.tajo.datum.exception.InvalidOperationException;
import org.apache.tajo.json.CommonGsonHelper;

import static org.apache.tajo.common.TajoDataTypes.Type;

public abstract class Datum implements Comparable<Datum>, GsonObject {
	@Expose	private Type type;
	
	@SuppressWarnings("unused")
  private Datum() {
	}
	
	public Datum(Type type) {
		this.type = type;
	}
	
	public Type type() {
		return this.type;
	}

  public boolean isNull() {
    return false;
  }
	
	public boolean asBool() {
    throw new InvalidCastException(type + " cannot be casted to BOOL type");
  }

  public byte asByte() {
    throw new InvalidCastException(type + " cannot be casted to BYTE type");
  }

  public char asChar() {
    throw new InvalidCastException(type + " cannot be casted to CHAR type");
  }

	public short asInt2() {
    throw new InvalidCastException(type + " cannot be casted to SHORT type");
  }
	public int asInt4() {
    throw new InvalidCastException(type + " cannot be casted to INT type");
  }

  public long asInt8() {
    throw new InvalidCastException(type + " cannot be casted to LONG type");
  }

	public byte [] asByteArray() {
    throw new InvalidCastException(type + " cannot be casted to BYTES type");
  }

	public float asFloat4() {
    throw new InvalidCastException(type + " cannot be casted to FLOAT type");
  }

	public double asFloat8() {
    throw new InvalidCastException(type + " cannot be casted to DOUBLE type");
  }

	public String asChars() {
    throw new InvalidCastException(type + " cannot be casted to STRING type");
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
	
	public abstract int size();
	
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
	
	public BooleanDatum equalsTo(Datum datum) {
    if (this instanceof NullDatum || datum instanceof NullDatum) {
    // TODO - comparing any value against null will be always unknown
      return DatumFactory.createBool(false);
    } else {
	    return DatumFactory.createBool(compareTo(datum) == 0);
    }
	}

	public BooleanDatum lessThan(Datum datum) {
    return DatumFactory.createBool(compareTo(datum) < 0);
	}
	
	public BooleanDatum lessThanEqual(Datum datum) {
    return DatumFactory.createBool(compareTo(datum) <= 0);
	}	
	
	public BooleanDatum greaterThan(Datum datum) {
    return DatumFactory.createBool(compareTo(datum) > 0);
	}
	
	public BooleanDatum greaterThanEqual(Datum datum) {
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
}
