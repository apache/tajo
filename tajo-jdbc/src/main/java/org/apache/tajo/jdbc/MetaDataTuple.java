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

import org.apache.tajo.datum.*;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.net.InetAddress;
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
    return values.get(fieldid) == null || values.get(fieldid) instanceof NullDatum;
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
    return getText(fieldId);
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
  public BooleanDatum getBoolean(int fieldId) {
    throw new UnsupportedException("getBoolean");
  }

  @Override
  public BitDatum getByte(int fieldId) {
    throw new UnsupportedException("getByte");
  }

  @Override
  public CharDatum getChar(int fieldId) {
    throw new UnsupportedException("getBoolean");
  }

  @Override
  public BlobDatum getBytes(int fieldId) {
    throw new UnsupportedException("BlobDatum");
  }

  @Override
  public Int2Datum getShort(int fieldId) {
    if(isNull(fieldId)) {
      return null;
    }
    return new Int2Datum((short)Integer.parseInt(values.get(fieldId).toString()));
  }

  @Override
  public Int4Datum getInt(int fieldId) {
    if(isNull(fieldId)) {
      return null;
    }
    return new Int4Datum(Integer.parseInt(values.get(fieldId).toString()));
  }

  @Override
  public Int8Datum getLong(int fieldId) {
    if(isNull(fieldId)) {
      return null;
    }
    return new Int8Datum(Long.parseLong(values.get(fieldId).toString()));
  }

  @Override
  public Float4Datum getFloat(int fieldId) {
    if(isNull(fieldId)) {
      return null;
    }
    return new Float4Datum(Float.parseFloat(values.get(fieldId).toString()));
  }

  @Override
  public Float8Datum getDouble(int fieldId) {
    if(isNull(fieldId)) {
      return null;
    }
    return new Float8Datum(Float.parseFloat(values.get(fieldId).toString()));
  }

  @Override
  public Inet4Datum getIPv4(int fieldId) {
    throw new UnsupportedException("getIPv4");
  }

  @Override
  public byte[] getIPv4Bytes(int fieldId) {
    throw new UnsupportedException("getIPv4Bytes");
  }

  @Override
  public InetAddress getIPv6(int fieldId) {
    throw new UnsupportedException("getIPv6");
  }

  @Override
  public byte[] getIPv6Bytes(int fieldId) {
    throw new UnsupportedException("getIPv6Bytes");
  }

  @Override
  public TextDatum getString(int fieldId) {
    if(isNull(fieldId)) {
      return null;
    }
    return new TextDatum(values.get(fieldId).toString());
  }

  @Override
  public TextDatum getText(int fieldId) {
    if(isNull(fieldId)) {
      return null;
    }
    return new TextDatum(values.get(fieldId).toString());
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    throw new UnsupportedException("clone");
  }

  @Override
  public Datum[] getValues(){
    throw new UnsupportedException();
  }
}
