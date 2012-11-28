/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.datum;

import com.google.gson.annotations.Expose;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import tajo.datum.exception.InvalidCastException;
import tajo.datum.exception.InvalidOperationException;
import tajo.datum.json.GsonCreator;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;

public class StringDatum2 extends Datum {
  @Expose
  private int size;
  @Expose
  private byte[] bytes;

  public StringDatum2() {
    super(DatumType.STRING2);
  }

  public StringDatum2(byte[] bytes) {
    this();
    this.bytes = bytes;
    this.size = bytes.length;
  }

  public StringDatum2(String string) {
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
  public short asShort() {
    throw new InvalidCastException();
  }

  @Override
  public int asInt() {
    throw new InvalidCastException();
  }

  @Override
  public long asLong() {
    throw new InvalidCastException();
  }

  @Override
  public float asFloat() {
    throw new InvalidCastException();
  }

  @Override
  public double asDouble() {
    throw new InvalidCastException();
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
      case STRING2:
        byte[] o = datum.asByteArray();
        return WritableComparator.compareBytes(this.bytes, 0, this.bytes.length,
            o, 0, o.length);
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StringDatum2) {
      StringDatum2 o = (StringDatum2) obj;
      return Arrays.equals(this.bytes, o.bytes);
    }

    return false;
  }

  @Override
  public BoolDatum equalsTo(Datum datum) {
    switch (datum.type()) {
      case STRING2:
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
    return bytes.hashCode();
  }
}
