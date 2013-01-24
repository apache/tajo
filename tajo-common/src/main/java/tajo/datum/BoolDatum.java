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
import tajo.datum.exception.InvalidOperationException;
import tajo.datum.json.GsonCreator;

public class BoolDatum extends Datum {
	@Expose private boolean val;

  public BoolDatum() {
    super(DatumType.BOOLEAN);
  }

	public BoolDatum(boolean val) {
		this();
		this.val = val;
	}

  public BoolDatum(byte byteVal) {
    this();
    this.val = byteVal == 1;
  }

  public BoolDatum(int byteVal) {
    this();
    this.val = byteVal == 1;
  }


  public BoolDatum(byte [] bytes) {
    this(bytes[0]);
  }
	
	public boolean asBool() {
		return val;
	}

  public void setValue(boolean val) {
    this.val = val;
  }
	
	@Override
	public short asShort() {	
		return (short) (val ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asInt()
	 */
	@Override
	public int asInt() {
		return val ? 1 : 0;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	@Override
	public long asLong() {
		return val ? 1 : 0;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByte()
	 */
	@Override
	public byte asByte() {
		return (byte) (val ? 0x01 : 0x00);
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	@Override
	public byte[] asByteArray() {
	  byte [] bytes = new byte[1];
    bytes[0] = asByte();
	  return bytes;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	@Override
	public float asFloat() {
		return val ? 1 : 0;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asDouble()
	 */
	@Override
	public double asDouble() {
		return val ? 1 : 0;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asChars()
	 */
	@Override
	public String asChars() {
		return val ? "true" : "false";
	}
	
	public String toJSON() {
		return GsonCreator.getInstance().toJson(this, Datum.class);
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
    if (obj instanceof BoolDatum) {
      BoolDatum other = (BoolDatum) obj;
      return val == other.val;
    }
    
    return false;
  }
  
  // Datum Comparator
  public BoolDatum equalsTo(Datum datum) {
    switch(datum.type()) {
      case BOOLEAN: return DatumFactory.createBool(this.val == 
          ((BoolDatum)datum).val);
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case BOOLEAN:
      if (val && !datum.asBool()) {
        return -1;
      } else if (val && datum.asBool()) {
        return 1;
      } else {
        return 0;
      }
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
}
