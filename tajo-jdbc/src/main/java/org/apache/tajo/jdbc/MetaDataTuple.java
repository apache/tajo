package org.apache.tajo.jdbc; /**
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

import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.util.ArrayList;
import java.util.List;

public class MetaDataTuple implements Tuple {
  List<Datum> values = new ArrayList<Datum>();

  public MetaDataTuple(int size) {
    values = new ArrayList<Datum>(size);
    for(int i = 0; i < size; i++) {
      values.add(NullDatum.get());
    }
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public boolean contains(int fieldid) {
    return false;
  }

  @Override
  public boolean isNull(int fieldid) {
    return values.get(fieldid) == null || values.get(fieldid).isNull();
  }

  @Override
  public boolean isNotNull(int fieldid) {
    return !isNull(fieldid);
  }

  @Override
  public void clear() {
    values.clear();
  }

  @Override
  public void put(int fieldId, Datum value) {
    values.set(fieldId, value);
  }

  @Override
  public void put(int fieldId, Datum[] values) {
    throw new UnsupportedException("put");
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    throw new UnsupportedException("put");
  }

  @Override
  public void put(Datum[] values) {
    throw new UnsupportedException("put");
  }

  @Override
  public Datum get(int fieldId) {
    return values.get(fieldId);
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedException("setOffset");
  }

  @Override
  public long getOffset() {
    throw new UnsupportedException("getOffset");
  }

  @Override
  public boolean getBool(int fieldId) {
    throw new UnsupportedException("getBool");
  }

  @Override
  public byte getByte(int fieldId) {
    throw new UnsupportedException("getByte");
  }

  @Override
  public char getChar(int fieldId) {
    throw new UnsupportedException("getChar");
  }

  @Override
  public byte [] getBytes(int fieldId) {
    throw new UnsupportedException("BlobDatum");
  }

  @Override
  public short getInt2(int fieldId) {
    return (short)Integer.parseInt(values.get(fieldId).toString());
  }

  @Override
  public int getInt4(int fieldId) {
    return Integer.parseInt(values.get(fieldId).toString());
  }

  @Override
  public long getInt8(int fieldId) {
    return Long.parseLong(values.get(fieldId).toString());
  }

  @Override
  public float getFloat4(int fieldId) {
    return Float.parseFloat(values.get(fieldId).toString());
  }

  @Override
  public double getFloat8(int fieldId) {
    return Float.parseFloat(values.get(fieldId).toString());
  }

  @Override
  public String getText(int fieldId) {
    return values.get(fieldId).toString();
  }

  @Override
  public ProtobufDatum getProtobufDatum(int fieldId) {
    throw new UnsupportedException("getProtobufDatum");
  }

  @Override
  public IntervalDatum getInterval(int fieldId) {
    throw new UnsupportedException("getInterval");
  }

  @Override
  public char[] getUnicodeChars(int fieldId) {
    return values.get(fieldId).asUnicodeChars();
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    throw new UnsupportedException("clone");
  }

  @Override
  public Datum[] getValues(){
    throw new UnsupportedException("getValues");
  }
}
