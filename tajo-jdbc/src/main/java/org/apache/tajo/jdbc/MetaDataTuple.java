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

package org.apache.tajo.jdbc;

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.TimeMeta;

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
  public boolean isBlank(int fieldid) {
    return values.get(fieldid) == null;
  }

  @Override
  public boolean isBlankOrNull(int fieldid) {
    return values.get(fieldid) == null || values.get(fieldid).isNull();
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    this.put(fieldId, tuple.asDatum(fieldId));
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
  public void put(Datum[] values) {
    for (int i = 0; i < values.length; i++) {
      this.values.set(i, values[i]);
    }
  }

  @Override
  public TajoDataTypes.Type type(int fieldId) {
    return values.get(fieldId).type();
  }

  @Override
  public int size(int fieldId) {
    return values.get(fieldId).size();
  }

  @Override
  public Datum asDatum(int fieldId) {
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
    return values.get(fieldId).asBool();
  }

  @Override
  public byte getByte(int fieldId) {
    return values.get(fieldId).asByte();
  }

  @Override
  public char getChar(int fieldId) {
    return values.get(fieldId).asChar();
  }

  @Override
  public byte [] getBytes(int fieldId) {
    throw new UnsupportedException("BlobDatum");
  }

  @Override
  public byte[] getTextBytes(int fieldId) {
    return values.get(fieldId).asTextBytes();
  }

  @Override
  public short getInt2(int fieldId) {
    return values.get(fieldId).asInt2();
  }

  @Override
  public int getInt4(int fieldId) {
    return values.get(fieldId).asInt4();
  }

  @Override
  public long getInt8(int fieldId) {
    return values.get(fieldId).asInt8();
  }

  @Override
  public float getFloat4(int fieldId) {
    return values.get(fieldId).asFloat4();
  }

  @Override
  public double getFloat8(int fieldId) {
    return values.get(fieldId).asFloat8();
  }

  @Override
  public String getText(int fieldId) {
    return values.get(fieldId).asChars();
  }

  @Override
  public TimeMeta getTimeDate(int fieldId) {
    return values.get(fieldId).asTimeMeta();
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
