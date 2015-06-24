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
package org.apache.tajo.storage;

import com.google.common.base.Preconditions;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.util.datetime.TimeMeta;

/**
 * An instance of FrameTuple is an immutable tuple.
 * It contains two tuples and pretends to be one instance of Tuple for
 * join qual evaluations.
 */
public class FrameTuple implements Tuple, Cloneable {
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

  public FrameTuple setLeft(Tuple left) {
    this.left = left;
    this.leftSize = left.size();
    return this;
  }

  public FrameTuple setRight(Tuple right) {
    this.right = right;
    this.size = leftSize + right.size();
    return this;
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
  public boolean isBlank(int fieldid) {
    return asDatum(fieldid) == null;
  }

  @Override
  public boolean isBlankOrNull(int fieldid) {
    Datum datum = asDatum(fieldid);
    return datum == null || datum.isNull();
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    throw new UnsupportedException();
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
  public void put(Datum[] values) {
    throw new UnsupportedException();
  }

  @Override
  public TajoDataTypes.Type type(int fieldId) {
    return null;
  }

  @Override
  public int size(int fieldId) {
    return 0;
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
  public Datum asDatum(int fieldId) {
    Preconditions.checkArgument(fieldId < size, 
        "Out of field access: " + fieldId);
    
    if (fieldId < leftSize) {
      return left.asDatum(fieldId);
    } else {
      return right.asDatum(fieldId - leftSize);
    }
  }

  @Override
  public boolean getBool(int fieldId) {
    return asDatum(fieldId).asBool();
  }

  @Override
  public byte getByte(int fieldId) {
    return asDatum(fieldId).asByte();
  }

  @Override
  public char getChar(int fieldId) {
    return asDatum(fieldId).asChar();
  }

  @Override
  public byte [] getBytes(int fieldId) {
    return asDatum(fieldId).asByteArray();
  }

  @Override
  public byte[] getTextBytes(int fieldId) {
    return asDatum(fieldId).asTextBytes();
  }

  @Override
  public short getInt2(int fieldId) {
    return asDatum(fieldId).asInt2();
  }

  @Override
  public int getInt4(int fieldId) {
    return asDatum(fieldId).asInt4();
  }

  @Override
  public long getInt8(int fieldId) {
    return asDatum(fieldId).asInt8();
  }

  @Override
  public float getFloat4(int fieldId) {
    return asDatum(fieldId).asFloat4();
  }

  @Override
  public double getFloat8(int fieldId) {
    return asDatum(fieldId).asFloat8();
  }

  @Override
  public String getText(int fieldId) {
    return asDatum(fieldId).asChars();
  }

  @Override
  public TimeMeta getTimeDate(int fieldId) {
    return null;
  }

  @Override
  public ProtobufDatum getProtobufDatum(int fieldId) {
    return (ProtobufDatum) asDatum(fieldId);
  }

  @Override
  public IntervalDatum getInterval(int fieldId) {
    return (IntervalDatum) asDatum(fieldId);
  }

  @Override
  public char [] getUnicodeChars(int fieldId) {
    return asDatum(fieldId).asUnicodeChars();
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    FrameTuple frameTuple = (FrameTuple) super.clone();
    frameTuple.set(this.left.clone(), this.right.clone());
    return frameTuple;
  }

  @Override
  public Datum[] getValues(){
    throw new UnsupportedException();
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
        .append(asDatum(i));
      }
    }
    str.append(")");
    return str.toString();
  }
}
