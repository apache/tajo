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
package tajo.storage;

import com.google.common.base.Preconditions;
import tajo.datum.*;
import tajo.exception.UnimplementedException;
import tajo.exception.UnsupportedException;

import java.net.InetAddress;

/**
 * An instance of FrameTuple is an immutable tuple.
 * It contains two tuples and pretends to be one instance of Tuple for
 * join qual evaluatations.
 */
public class FrameTuple implements Tuple {
  private int size;
  private int leftSize;
  
  private Tuple left;
  private Tuple right;
  
  public FrameTuple() {}
  
  public FrameTuple(Tuple left, Tuple right) {
    set(left, right);
  }
  
  public void set(Tuple left, Tuple right) {
    this.size = left.size() + right.size();
    this.left = left;
    this.leftSize = left.size();
    this.right = right;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean contains(int fieldId) {
    Preconditions.checkArgument(fieldId < size, 
        "Out of field access: " + fieldId);
    
    if (fieldId < leftSize) {
      return left.contains(fieldId);
    } else {
      return right.contains(fieldId - leftSize);
    }
  }

  @Override
  public boolean isNull(int fieldid) {
    return get(fieldid) instanceof NullDatum;
  }

  @Override
  public void clear() {
    throw new UnsupportedException();
  }

  @Override
  public void put(int fieldId, Datum value) {
    throw new UnsupportedException();
  }

  @Override
  public void put(int fieldId, Datum[] values) {
    throw new UnsupportedException();
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    throw new UnsupportedException();
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedException();
  }
  
  @Override
  public long getOffset() {
    throw new UnsupportedException();
  }

  @Override
  public void put(Datum [] values) {
    throw new UnsupportedException();
  }

  @Override
  public Datum get(int fieldId) {
    Preconditions.checkArgument(fieldId < size, 
        "Out of field access: " + fieldId);
    
    if (fieldId < leftSize) {
      return left.get(fieldId);
    } else {
      return right.get(fieldId - leftSize);
    }
  }

  @Override
  public BooleanDatum getBoolean(int fieldId) {
    return (BooleanDatum) get(fieldId);
  }

  @Override
  public BitDatum getByte(int fieldId) {
    return (BitDatum) get(fieldId);
  }

  @Override
  public CharDatum getChar(int fieldId) {
    return (CharDatum) get(fieldId);
  }

  @Override
  public BlobDatum getBytes(int fieldId) {
    return (BlobDatum) get(fieldId);
  }

  @Override
  public Int2Datum getShort(int fieldId) {
    return (Int2Datum) get(fieldId);
  }

  @Override
  public Int4Datum getInt(int fieldId) {
    return (Int4Datum) get(fieldId);
  }

  @Override
  public Int8Datum getLong(int fieldId) {
    return (Int8Datum) get(fieldId);
  }

  @Override
  public Float4Datum getFloat(int fieldId) {
    return (Float4Datum) get(fieldId);
  }

  @Override
  public Float8Datum getDouble(int fieldId) {
    return (Float8Datum) get(fieldId);
  }

  @Override
  public Inet4Datum getIPv4(int fieldId) {
    return (Inet4Datum) get(fieldId);
  }

  @Override
  public byte[] getIPv4Bytes(int fieldId) { 
    return get(fieldId).asByteArray();
  }

  @Override
  public InetAddress getIPv6(int fieldId) {
    throw new UnimplementedException();
  }
  
  @Override
  public byte[] getIPv6Bytes(int fieldId) {
    throw new UnimplementedException();
  }

  @Override
  public TextDatum getString(int fieldId) {
    return (TextDatum) get(fieldId);
  }

  @Override
  public TextDatum getText(int fieldId) {
    return (TextDatum) get(fieldId);
  }

  public String toString() {
    boolean first = true;
    StringBuilder str = new StringBuilder();
    str.append("(");
    for(int i=0; i < size(); i++) {      
      if(contains(i)) {
        if(first) {
          first = false;
        } else {
          str.append(", ");
        }
        str.append(i)
        .append("=>")
        .append(get(i));
      }
    }
    str.append(")");
    return str.toString();
  }
}
