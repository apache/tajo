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

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedInteger;
import com.google.gson.annotations.Expose;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class Inet4Datum extends Datum {
  private static final int size = 4;
  @Expose private final int address;

  Inet4Datum(int encoded) {
    super(Type.INET4);
    this.address = encoded;
  }

	public Inet4Datum(String addr) {
    super(Type.INET4);
		String [] elems = addr.split("\\.");
    address = Integer.parseInt(elems[3]) & 0xFF
        | ((Integer.parseInt(elems[2]) << 8) & 0xFF00)
        | ((Integer.parseInt(elems[1]) << 16) & 0xFF0000)
        | ((Integer.parseInt(elems[0]) << 24) & 0xFF000000);
  }

	public Inet4Datum(byte[] addr) {
    super(Type.INET4);
		Preconditions.checkArgument(addr.length == size);
    address = addr[3] & 0xFF
        | ((addr[2] << 8) & 0xFF00)
        | ((addr[1] << 16) & 0xFF0000)
        | ((addr[0] << 24) & 0xFF000000);
  }

  public Inet4Datum(byte[] addr, int offset, int length) {
    super(Type.INET4);
    Preconditions.checkArgument(length == size);
    address = addr[offset + 3] & 0xFF
        | ((addr[offset + 2] << 8) & 0xFF00)
        | ((addr[offset + 1] << 16) & 0xFF0000)
        | ((addr[offset] << 24) & 0xFF000000);
  }

	@Override
	public int asInt4() {
		return this.address;
	}

	@Override
	public long asInt8() {
	  return UnsignedInteger.fromIntBits(address).longValue();
	}

	@Override
	public byte[] asByteArray() {
	  byte[] addr = new byte[size];
	  addr[0] = (byte) ((address >>> 24) & 0xFF);
	  addr[1] = (byte) ((address >>> 16) & 0xFF);
	  addr[2] = (byte) ((address >>> 8) & 0xFF);
	  addr[3] = (byte) (address & 0xFF);
	  return addr;
	}

	@Override
	public String asChars() {
		return numericToTextFormat(asByteArray());
	}

  @Override
  public int size() {
    return size;
  }

  @Override
  public int hashCode() {
    return address;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Inet4Datum) {
      Inet4Datum other = (Inet4Datum) obj;
      return this.address == other.address;
    }

    return false;
  }

  @Override
  public Datum equalsTo(Datum datum) {
    switch (datum.type()) {
      case INET4:
        return DatumFactory.createBool(this.address == ((Inet4Datum) datum).address);
      case NULL_TYPE:
        return datum;
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
      case INET4:
        byte[] bytes = asByteArray();
        byte[] other = datum.asByteArray();
        return Bytes.compareTo(bytes, 0, size, other, 0, size);
      case NULL_TYPE:
        return -1;
      default:
        throw new InvalidOperationException(datum.type());
    }
  }
  
  static String numericToTextFormat(byte[] src) {
    return (src[0] & 0xff) + "." + (src[1] & 0xff) + "." + (src[2] & 0xff)
        + "." + (src[3] & 0xff);
  }
}
