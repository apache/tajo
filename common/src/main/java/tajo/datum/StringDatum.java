/*
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
import tajo.datum.json.GsonCreator;


public class StringDatum extends Datum { 
	@Expose String val;
	
	public StringDatum() {
    super(DatumType.STRING);
  }

	public StringDatum(String val) {
		this();
		this.val = val;
	}

  public StringDatum(byte [] bytes) {
    this();
    this.val = new String(bytes);
  }

	@Override
	public boolean asBool() {	
		throw new InvalidCastException();
	}

	@Override
	public byte asByte() {
		throw new InvalidCastException();
	}
	
	@Override
	public short asShort() {	
		throw new InvalidCastException();
	}

	@Override
	public int asInt() {
		int res;
		try {
			res = Integer.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	@Override
	public long asLong() {
		long res;
		try {
			res = Long.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	@Override
	public byte[] asByteArray() {		
		return val.getBytes();
	}

	@Override
	public float asFloat() {
		float res;
		try {
			res = Float.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	@Override
	public double asDouble() {
		double res;
		try {
			res = Double.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	@Override
	public String asChars() {
		return val;
	}

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StringDatum && this.val.equals(((StringDatum) obj).val)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int size() {
    return val.getBytes().length;
  }
  
  @Override
  public int hashCode() {
    return val.hashCode();
  }

  @Override
  public BoolDatum equalsTo(Datum datum) {
    switch (datum.type()) {
    case STRING:
      return DatumFactory
          .createBool(this.val.equals(((StringDatum) datum).val));
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case STRING:
      return this.val.compareTo(((StringDatum) datum).val);
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public String toJSON() {
		return GsonCreator.getInstance().toJson(this, Datum.class);
	}
}
