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

package org.apache.tajo.storage;

import com.google.gson.annotations.Expose;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Inet4Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.UnimplementedException;

import java.net.InetAddress;
import java.util.Arrays;

public class VTuple implements Tuple, Cloneable {
	@Expose public Datum [] values;
	@Expose private long offset;
	
	public VTuple(int size) {
		values = new Datum[size];
	}

  public VTuple(Tuple tuple) {
    this.values = tuple.getValues().clone();
  }

  public VTuple(Datum[] datum) {
    this(datum.length);
    put(datum);
  }

	@Override
	public int size() {	
		return values.length;
	}
	
	public boolean contains(int fieldId) {
		return values[fieldId] != null;
	}

  @Override
  public boolean isNull(int fieldid) {
    return values[fieldid].isNull();
  }

  @Override
  public boolean isNotNull(int fieldid) {
    return !isNull(fieldid);
  }

  @Override
  public void clear() {   
    for (int i=0; i < values.length; i++) {
      values[i] = null;
    }
  }
	
	//////////////////////////////////////////////////////
	// Setter
	//////////////////////////////////////////////////////	
	public void put(int fieldId, Datum value) {
		values[fieldId] = value;
	}

  @Override
  public void put(int fieldId, Datum[] values) {
    for (int i = fieldId, j = 0; j < values.length; i++, j++) {
      values[i] = values[j];
    }
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    for (int i = fieldId, j = 0; j < tuple.size(); i++, j++) {
      values[i] = tuple.get(j);
    }
  }

  public void put(Datum [] values) {
    System.arraycopy(values, 0, this.values, 0, size());
	}
	
	//////////////////////////////////////////////////////
	// Getter
	//////////////////////////////////////////////////////
	public Datum get(int fieldId) {
		return this.values[fieldId];
	}
	
	public void setOffset(long offset) {
	  this.offset = offset;
	}
	
	public long getOffset() {
	  return this.offset;
	}
	
	@Override
	public boolean getBool(int fieldId) {
		return values[fieldId].asBool();
	}

  @Override
	public byte getByte(int fieldId) {
		return values[fieldId].asByte();
	}

  @Override
  public char getChar(int fieldId) {
    return values[fieldId].asChar();
  }

  @Override
	public byte [] getBytes(int fieldId) {
		return values[fieldId].asByteArray();
	}

  @Override
	public short getInt2(int fieldId) {
		return values[fieldId].asInt2();
	}

  @Override
	public int getInt4(int fieldId) {
		return values[fieldId].asInt4();
	}

  @Override
	public long getInt8(int fieldId) {
		return values[fieldId].asInt8();
	}

  @Override
	public float getFloat4(int fieldId) {
		return values[fieldId].asFloat4();
	}

  @Override
	public double getFloat8(int fieldId) {
		return values[fieldId].asFloat8();
	}

	public Inet4Datum getIPv4(int fieldId) {
		return (Inet4Datum) values[fieldId];
	}

	public byte [] getIPv4Bytes(int fieldId) {
		return values[fieldId].asByteArray();
	}

	public InetAddress getIPv6(int fieldId) {
		throw new UnimplementedException("IPv6 is unsupported yet");
	}

	public byte[] getIPv6Bytes(int fieldId) {
	  throw new UnimplementedException("IPv6 is unsupported yet");
	}

  @Override
	public String getText(int fieldId) {
		return values[fieldId].asChars();
	}

  @Override
  public ProtobufDatum getProtobufDatum(int fieldId) {
    return (ProtobufDatum) values[fieldId];
  }

  @Override
  public IntervalDatum getInterval(int fieldId) {
    return (IntervalDatum) values[fieldId];
  }

  @Override
  public char[] getUnicodeChars(int fieldId) {
    return values[fieldId].asUnicodeChars();
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    VTuple tuple = (VTuple) super.clone();

    tuple.values = new Datum[size()];
    System.arraycopy(values, 0, tuple.values, 0, size()); //shallow copy
    return tuple;
  }

  public String toString() {
		return toDisplayString(getValues());
	}

	@Override
	public int hashCode() {
	  return Arrays.hashCode(values);
	}

  @Override
  public Datum[] getValues() {
    return values;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Tuple) {
      Tuple other = (Tuple) obj;
      return Arrays.equals(getValues(), other.getValues());
    }
    return false;
  }

  public static String toDisplayString(Datum [] values) {
    boolean first = true;
    StringBuilder str = new StringBuilder();
    str.append("(");
    for(int i=0; i < values.length; i++) {
      if(values[i] != null) {
        if(first) {
          first = false;
        } else {
          str.append(", ");
        }
        str.append(i)
            .append("=>")
            .append(values[i]);
      }
    }
    str.append(")");
    return str.toString();
  }
}
