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

import com.google.gson.annotations.Expose;
import org.apache.hadoop.io.WritableComparator;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.exception.InvalidCastException;
import org.apache.tajo.datum.exception.InvalidOperationException;
import org.apache.tajo.datum.json.GsonCreator;

import java.util.Arrays;

public class TextDatum extends Datum {
  @Expose
  private int size;
  @Expose
  private byte[] bytes;

  public TextDatum() {
    super(TajoDataTypes.Type.TEXT);
  }

  public TextDatum(byte[] bytes) {
    this();
    this.bytes = bytes;
    this.size = bytes.length;
  }

  public TextDatum(String string) {
    this(string.getBytes());
  }

  @Override
  public boolean asBool() {
    throw new InvalidCastException();
  }

  @Override
  public byte asByte() {
    throw new InvalidCastException();
  }

  @Override
  public short asInt2() {
    return Short.valueOf(new String(bytes));
  }

  @Override
  public int asInt4() {
    return Integer.valueOf(new String(bytes));
  }

  @Override
  public long asInt8() {
    return Long.valueOf(new String(bytes));
  }

  @Override
  public float asFloat4() {
    return Float.valueOf(new String(bytes));
  }

  @Override
  public double asFloat8() {
    return Double.valueOf(new String(bytes));
  }

  @Override
  public byte[] asByteArray() {
    return this.bytes;
  }

  public String asChars() {
    return new String(this.bytes);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
      case TEXT:
        byte[] o = datum.asByteArray();
        return WritableComparator.compareBytes(this.bytes, 0, this.bytes.length,
            o, 0, o.length);
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TextDatum) {
      TextDatum o = (TextDatum) obj;
      return Arrays.equals(this.bytes, o.bytes);
    }

    return false;
  }

  @Override
  public BooleanDatum equalsTo(Datum datum) {
    switch (datum.type()) {
      case TEXT:
        return DatumFactory.createBool(
            Arrays.equals(this.bytes, datum.asByteArray()));
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, Datum.class);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public String toString() {
    return asChars();
  }
}
