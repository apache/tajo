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

package tajo.datum;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import tajo.datum.exception.InvalidOperationException;
import tajo.datum.json.GsonCreator;

public class IPv4Datum extends Datum {
  private static final int size = 4;
  @Expose private int address;

	public IPv4Datum() {
		super(DatumType.IPv4);
	}
	
	public IPv4Datum(String addr) {
		this();
		String [] elems = addr.split("\\.");
		address  = Integer.valueOf(elems[3]) & 0xFF;
    address |= ((Integer.valueOf(elems[2]) << 8) & 0xFF00);
    address |= ((Integer.valueOf(elems[1]) << 16) & 0xFF0000);
    address |= ((Integer.valueOf(elems[0]) << 24) & 0xFF000000);
	}
	
	public IPv4Datum(byte [] addr) {
		this();
		Preconditions.checkArgument(addr.length == size);
		address  = addr[3] & 0xFF;
    address |= ((addr[2] << 8) & 0xFF00);
    address |= ((addr[1] << 16) & 0xFF0000);
    address |= ((addr[0] << 24) & 0xFF000000);
	}

	@Override
	public int asInt() {
		return this.address;
	}

	@Override
	public long asLong() {
	  return this.address;
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
	public String toJSON() {
		return GsonCreator.getInstance().toJson(this, Datum.class);
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
    if (obj instanceof IPv4Datum) {
      IPv4Datum other = (IPv4Datum) obj;
      return this.address == other.address;
    }
    
    return false;
  }

  @Override
  public BoolDatum equalsTo(Datum datum) {
    switch (datum.type()) {
    case IPv4:    	
    	return DatumFactory.createBool(this.address == ((IPv4Datum)datum).address);
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case IPv4:
      byte [] bytes = asByteArray();
      byte [] other = datum.asByteArray();
      
      for (int i = 0; i < 4; i++) {
        if (bytes[i] > other[i]) {
          return 1;
        } else if (bytes[i] < other[i]) {
          return -1;
        }
      }
      
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  static String numericToTextFormat(byte[] src) {
    return (src[0] & 0xff) + "." + (src[1] & 0xff) + "." + (src[2] & 0xff)
        + "." + (src[3] & 0xff);
  }
}
