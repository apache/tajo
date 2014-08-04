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

import com.google.common.primitives.UnsignedBytes;
import com.google.gson.annotations.Expose;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InvalidCastException;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.MurmurHash;

import java.util.Comparator;

public class TextDatum extends Datum {
  @Expose private final int size;
  @Expose private final byte[] bytes;

  public static final TextDatum EMPTY_TEXT = new TextDatum("");
  public static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

  public TextDatum(byte[] bytes) {
    super(TajoDataTypes.Type.TEXT);
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
    return Integer.parseInt(new String(bytes));
  }

  @Override
  public long asInt8() {
    return Long.parseLong(new String(bytes));
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
  public byte[] asTextBytes() {
    return this.bytes;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
      case TEXT:
      case CHAR:
      case BLOB:
        return COMPARATOR.compare(bytes, datum.asByteArray());

      case NULL_TYPE:
        return -1;

      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TextDatum) {
      TextDatum o = (TextDatum) obj;
      return COMPARATOR.compare(this.bytes, o.bytes) == 0;
    }

    return false;
  }

  @Override
  public Datum equalsTo(Datum datum) {
    switch (datum.type()) {
      case TEXT:
      case CHAR:
      case BLOB:
        return DatumFactory.createBool(COMPARATOR.compare(bytes, datum.asByteArray()) == 0);
      case NULL_TYPE:
        return datum;
      default:
        throw new InvalidOperationException("Cannot equivalent check: " + this.type() + " and " + datum.type());
    }
  }

  @Override
  public int hashCode() {
    return MurmurHash.hash(bytes);
  }

  @Override
  public String toString() {
    return asChars();
  }
}
