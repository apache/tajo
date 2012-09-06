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

import com.google.gson.annotations.Expose;
import tajo.datum.exception.InvalidCastException;
import tajo.datum.exception.InvalidOperationException;

public abstract class Datum implements Comparable<Datum> {
	@Expose
	private DatumType type;
	
	@SuppressWarnings("unused")
  private Datum() {
	}
	
	public Datum(DatumType type) {
		this.type = type;
	}
	
	public DatumType type() {
		return this.type;
	}

  public boolean isNull() {
    return type == DatumType.NULL;
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

	public short asShort() {
    throw new InvalidCastException(type + " cannot be casted to SHORT type");
  }
	public int asInt() {
    throw new InvalidCastException(type + " cannot be casted to INT type");
  }

  public long asLong() {
    throw new InvalidCastException(type + " cannot be casted to LONG type");
  }

	public byte [] asByteArray() {
    throw new InvalidCastException(type + " cannot be casted to BYTES type");
  }

	public float asFloat() {
    throw new InvalidCastException(type + " cannot be casted to FLOAT type");
  }

	public double asDouble() {
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
	      this.type == DatumType.SHORT ||
	      this.type == DatumType.INT ||
	      this.type == DatumType.LONG;
	}
	
	public boolean isReal() {
    return 
        this.type == DatumType.FLOAT ||
        this.type == DatumType.DOUBLE;
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
	
	public BoolDatum equalsTo(Datum datum) {
    if (this.isNull() || datum.isNull()) {
    // TODO - comparing any value against null will be always unknown
      return DatumFactory.createBool(false);
    } else {
	    return DatumFactory.createBool(compareTo(datum) == 0);
    }
	}

	public BoolDatum lessThan(Datum datum) {
    return DatumFactory.createBool(compareTo(datum) < 0);
	}
	
	public BoolDatum lessThanEqual(Datum datum) {
    return DatumFactory.createBool(compareTo(datum) <= 0);
	}	
	
	public BoolDatum greaterThan(Datum datum) {
    return DatumFactory.createBool(compareTo(datum) > 0);
	}
	
	public BoolDatum greaterThanEqual(Datum datum) {
    return DatumFactory.createBool(compareTo(datum) >= 0);
	}
	
  public abstract int compareTo(Datum datum);

  public abstract String toJSON();

  @Override
  public String toString() {
    return asChars();
  }
}
