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

import com.google.common.primitives.UnsignedBytes;
import com.google.gson.annotations.Expose;
import org.apache.tajo.exception.InvalidOperationException;

import java.util.Arrays;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class CharDatum extends Datum {
  @Expose private final int size;
  @Expose private final byte[] bytes;
  private String chars = null;

	public CharDatum(byte val) {
    super(Type.CHAR);
    this.size = 1;
    bytes = new byte[size];
    bytes[0] = val;
	}

  public CharDatum(char val) {
    this((byte)val);
  }

  public CharDatum(byte [] bytes) {
    super(Type.CHAR);
    this.bytes = bytes;
    this.size = bytes.length;
  }

  public CharDatum(String val) {
    this(val.getBytes());
  }

  private String getString() {
    if (chars == null) {
      chars = new String(bytes);
    }
    return chars;
  }

  @Override
  public char asChar() {
    return getString().charAt(0);
  }

  @Override
  public short asInt2() {
    return Short.valueOf(getString());
  }

  @Override
	public int asInt4() {
		return Integer.parseInt(getString());
	}

  @Override
	public long asInt8() {
		return Long.parseLong(getString());
	}

  @Override
	public byte[] asByteArray() {
		return bytes;
	}

  @Override
	public float asFloat4() {
    return Float.valueOf(getString());
	}

  @Override
	public double asFloat8() {
		return Double.valueOf(getString());
	}

  @Override
  public byte asByte() {
    return bytes[0];
  }

  @Override
	public String asChars() {
		return getString();
	}

  /**
   * Return the real length of the string
   * @return the length of the string
   */
  @Override
  public int size() {
    return size;
  }
  
  @Override
  public int hashCode() {
    return getString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CharDatum) {
      CharDatum other = (CharDatum) obj;
      return this.size == other.size &&
          Arrays.equals(this.bytes, other.bytes);
    }
    
    return false;
  }

  @Override
  public Datum equalsTo(Datum datum) {
    switch (datum.type()) {
      case CHAR:
      case VARCHAR:
      case TEXT:
        return DatumFactory.createBool(TextDatum.COMPARATOR.compare(bytes, datum.asTextBytes()) == 0);

      case NULL_TYPE:
        return datum;

      default:
        throw new InvalidOperationException(datum.type());
    }
  }
  
  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
      case CHAR:
        CharDatum other = (CharDatum) datum;
        return UnsignedBytes.lexicographicalComparator().compare(bytes, other.bytes);

      case NULL_TYPE:
        return -1;

      default:
        throw new InvalidOperationException(datum.type());
    }
  }
}
