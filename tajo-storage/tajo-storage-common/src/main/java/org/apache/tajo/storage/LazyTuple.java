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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.UnsupportedException;

import java.util.Arrays;

public class LazyTuple implements Tuple, Cloneable {
  private long offset;
  private Datum[] values;
  private byte[][] textBytes;
  private Schema schema;
  private byte[] nullBytes;
  private SerializerDeserializer serializeDeserialize;

  public LazyTuple(Schema schema, byte[][] textBytes, long offset) {
    this(schema, textBytes, offset, NullDatum.get().asTextBytes(), new TextSerializerDeserializer());
  }

  public LazyTuple(Schema schema, byte[][] textBytes, long offset, byte[] nullBytes, SerializerDeserializer serde) {
    this.schema = schema;
    this.textBytes = textBytes;
    this.values = new Datum[schema.size()];
    this.offset = offset;
    this.nullBytes = nullBytes;
    this.serializeDeserialize = serde;
  }

  public LazyTuple(LazyTuple tuple) {
    this.values = tuple.getValues();
    this.offset = tuple.offset;
    this.schema = tuple.schema;
    this.textBytes = new byte[size()][];
    this.nullBytes = tuple.nullBytes;
    this.serializeDeserialize = tuple.serializeDeserialize;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public boolean contains(int fieldid) {
    return textBytes[fieldid] != null || values[fieldid] != null;
  }

  @Override
  public boolean isNull(int fieldid) {
    return get(fieldid).isNull();
  }

  @Override
  public boolean isNotNull(int fieldid) {
    return !isNull(fieldid);
  }

  @Override
  public void clear() {
    for (int i = 0; i < values.length; i++) {
      values[i] = null;
      textBytes[i] = null;
    }
  }

  //////////////////////////////////////////////////////
  // Setter
  //////////////////////////////////////////////////////
  @Override
  public void put(int fieldId, Datum value) {
    values[fieldId] = value;
    textBytes[fieldId] = null;
  }

  @Override
  public void put(int fieldId, Datum[] values) {
    for (int i = fieldId, j = 0; j < values.length; i++, j++) {
      this.values[i] = values[j];
    }
    this.textBytes = new byte[values.length][];
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    for (int i = fieldId, j = 0; j < tuple.size(); i++, j++) {
      values[i] = tuple.get(j);
      textBytes[i] = null;
    }
  }

  @Override
  public void put(Datum[] values) {
    System.arraycopy(values, 0, this.values, 0, size());
    this.textBytes = new byte[values.length][];
  }

  //////////////////////////////////////////////////////
  // Getter
  //////////////////////////////////////////////////////
  @Override
  public Datum get(int fieldId) {
    if (values[fieldId] != null)
      return values[fieldId];
    else if (textBytes.length <= fieldId) {
      values[fieldId] = NullDatum.get();  // split error. (col : 3, separator: ',', row text: "a,")
    } else if (textBytes[fieldId] != null) {
      try {
        values[fieldId] = serializeDeserialize.deserialize(schema.getColumn(fieldId),
            textBytes[fieldId], 0, textBytes[fieldId].length, nullBytes);
      } catch (Exception e) {
        values[fieldId] = NullDatum.get();
      }
      textBytes[fieldId] = null;
    } else {
      //non-projection
    }
    return values[fieldId];
  }

  @Override
  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public long getOffset() {
    return this.offset;
  }

  @Override
  public boolean getBool(int fieldId) {
    return get(fieldId).asBool();
  }

  @Override
  public byte getByte(int fieldId) {
    return get(fieldId).asByte();
  }

  @Override
  public char getChar(int fieldId) {
    return get(fieldId).asChar();
  }

  @Override
  public byte [] getBytes(int fieldId) {
    return get(fieldId).asByteArray();
  }

  @Override
  public short getInt2(int fieldId) {
    return get(fieldId).asInt2();
  }

  @Override
  public int getInt4(int fieldId) {
    return get(fieldId).asInt4();
  }

  @Override
  public long getInt8(int fieldId) {
    return get(fieldId).asInt8();
  }

  @Override
  public float getFloat4(int fieldId) {
    return get(fieldId).asFloat4();
  }

  @Override
  public double getFloat8(int fieldId) {
    return get(fieldId).asFloat8();
  }

  @Override
  public String getText(int fieldId) {
    return get(fieldId).asChars();
  }

  @Override
  public ProtobufDatum getProtobufDatum(int fieldId) {
    throw new UnsupportedException();
  }

  @Override
  public IntervalDatum getInterval(int fieldId) {
    return (IntervalDatum) get(fieldId);
  }

  @Override
  public char[] getUnicodeChars(int fieldId) {
    return get(fieldId).asUnicodeChars();
  }

  public String toString() {
    boolean first = true;
    StringBuilder str = new StringBuilder();
    str.append("(");
    Datum d;
    for (int i = 0; i < values.length; i++) {
      d = get(i);
      if (d != null) {
        if (first) {
          first = false;
        } else {
          str.append(", ");
        }
        str.append(i)
            .append("=>")
            .append(d);
      }
    }
    str.append(")");
    return str.toString();
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override
  public Datum[] getValues() {
    Datum[] datums = new Datum[values.length];
    for (int i = 0; i < values.length; i++) {
      datums[i] = get(i);
    }
    return datums;
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    LazyTuple lazyTuple = (LazyTuple) super.clone();

    lazyTuple.values = getValues(); //shallow copy
    lazyTuple.textBytes = new byte[size()][];
    return lazyTuple;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Tuple) {
      Tuple other = (Tuple) obj;
      return Arrays.equals(getValues(), other.getValues());
    }
    return false;
  }
}
