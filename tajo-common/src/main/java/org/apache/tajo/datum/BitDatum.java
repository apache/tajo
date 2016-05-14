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
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.type.Type;
import org.apache.tajo.util.NumberUtil;

public class BitDatum extends Datum {
  private static final int size = 1;
  @Expose	final byte val;
	
	public BitDatum(byte val) {
		super(Type.Bit);
		this.val = val;
	}

  public BitDatum(byte[] bytes) {
    this(bytes[0]);
  }

  @Override
  public char asChar() {
    return (char)val;
  }

	@Override
	public int asInt4() {
		return val;
	}

  @Override
	public long asInt8() {
		return val;
	}

  @Override
	public byte asByte() {
		return val;
	}

  @Override
	public byte[] asByteArray() {
    byte [] bytes = new byte[1];
    bytes[0] = this.val;
		return bytes;
	}

  @Override
	public float asFloat4() {
		return val;
	}

  @Override
	public double asFloat8() {
		return val;
	}

  @Override
	public String asChars() {
		return "0x"+val;
	}

  @Override
  public byte[] asTextBytes() {
    return NumberUtil.toAsciiBytes(asInt4());
  }

  @Override
  public int size() {
    return size;
  }
  
  @Override
  public int hashCode() {
    return val;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BitDatum) {
      BitDatum other = (BitDatum) obj;
      return val == other.val;
    }
    
    return false;
  }

  @Override
  public Datum equalsTo(Datum datum) {
    switch (datum.kind()) {
    case BIT:
      return DatumFactory.createBool(this.val == (((BitDatum) datum).val));
    case NULL_TYPE:
      return datum;
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  @Override
  public int compareTo(Datum datum) {
    switch (datum.kind()) {
    case BIT:
      if (val < datum.asByte() ) {
        return -1;
      } else if (val > datum.asByte()) {
        return 1;
      } else {
        return 0;
      }
    case NULL_TYPE:
      return -1;
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
}
