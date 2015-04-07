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
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.TUtil;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.apache.tajo.common.TajoDataTypes.Type.BLOB;

public class BlobDatum extends Datum {
	@Expose private final byte [] val;
	private ByteBuffer bb = null;

	public BlobDatum(byte[] val) {
    super(BLOB);
		this.val = val;
		this.bb = ByteBuffer.wrap(val);	
		bb.flip();
	}

  public BlobDatum(byte[] val, int offset, int length) {
    super(BLOB);
    byte[] b = new byte[length];
    System.arraycopy(val, offset, b, 0 , length);
    this.val = b;
    this.bb = ByteBuffer.wrap(b);
    bb.flip();
  }
	
	public BlobDatum(ByteBuffer val) {
    super(BLOB);
		this.val = val.array();
		this.bb = val.duplicate();
		bb.flip();
	}
	
	public void initFromBytes() {
		if (bb == null) {
			bb = ByteBuffer.wrap(val);
		}
	}

  @Override
	public int asInt4() {
		initFromBytes();
		bb.rewind();
		return bb.getInt();
	}

  @Override
	public long asInt8() {
		initFromBytes();
		bb.rewind();
		return bb.getLong();
	}

  @Override
	public byte asByte() {
		initFromBytes();
		bb.rewind();
		return bb.get();
	}

  @Override
	public byte[] asByteArray() {
		initFromBytes();
		bb.rewind();
		return bb.array();
	}

  @Override
	public float asFloat4() {
		initFromBytes();
		bb.rewind();
		return bb.getFloat();
	}

  @Override
	public double asFloat8() {
		initFromBytes();
		bb.rewind();
		return bb.getDouble();
	}

  @Override
	public String asChars() {
		initFromBytes();
		bb.rewind();
		return new String(bb.array(), Charset.defaultCharset());
	}

  @Override
  public int size() {
	  return this.val.length;
  }
  
  @Override
  public int hashCode() {
	  initFromBytes();
	  bb.rewind();
    return bb.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BlobDatum) {
      BlobDatum other = (BlobDatum) obj;
      initFromBytes();
      other.initFromBytes();
			return Arrays.equals(this.val, other.val);
		}
    
    return false;
  }

  @Override
  public Datum equalsTo(Datum datum) {
    switch (datum.type()) {
    case BLOB:
      return DatumFactory.createBool(Arrays.equals(this.val, ((BlobDatum)datum).val));
    case NULL_TYPE:
      return datum;
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case BLOB:
    	initFromBytes();
    	((BlobDatum)datum).initFromBytes();
      return bb.compareTo(((BlobDatum) datum).bb);
    case NULL_TYPE:
      return -1;
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
}
