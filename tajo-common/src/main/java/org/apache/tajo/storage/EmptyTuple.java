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

import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;

/* This class doesn’t have content datum. if selected column is zero, this is useful
*  e.g. select count(*) from table
* */
public class EmptyTuple implements Tuple, Cloneable {

  private static EmptyTuple tuple;
  private static Datum[] EMPTY_VALUES = new Datum[0];

  static {
    tuple = new EmptyTuple();
  }

  public static EmptyTuple get() {
    return tuple;
  }

  private EmptyTuple() {
  }

  @Override
  public int size() {
    return 0;
  }

  public boolean contains(int fieldId) {
    return false;
  }

  @Override
  public boolean isNull(int fieldid) {
    return true;
  }

  @Override
  public boolean isNotNull(int fieldid) {
    return false;
  }

  @Override
  public void clear() {
  }

  @Override
  public void put(int fieldId, Datum value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(int fieldId, Datum[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(Datum[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Datum get(int fieldId) {
    return NullDatum.get();
  }

  @Override
  public void setOffset(long offset) {

  }

  @Override
  public long getOffset() {
    return -1;
  }

  @Override
  public boolean getBool(int fieldId) {
    return NullDatum.get().asBool();
  }

  @Override
  public byte getByte(int fieldId) {
    return NullDatum.get().asByte();
  }

  @Override
  public char getChar(int fieldId) {
    return NullDatum.get().asChar();
  }

  @Override
  public byte[] getBytes(int fieldId) {
    return NullDatum.get().asByteArray();
  }

  @Override
  public short getInt2(int fieldId) {
    return NullDatum.get().asInt2();
  }

  @Override
  public int getInt4(int fieldId) {
    return NullDatum.get().asInt4();
  }

  @Override
  public long getInt8(int fieldId) {
    return NullDatum.get().asInt8();
  }

  @Override
  public float getFloat4(int fieldId) {
    return NullDatum.get().asFloat4();
  }

  @Override
  public double getFloat8(int fieldId) {
    return NullDatum.get().asFloat8();
  }

  @Override
  public String getText(int fieldId) {
    return NullDatum.get().asChars();
  }

  @Override
  public ProtobufDatum getProtobufDatum(int fieldId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Datum getInterval(int fieldId) {
    return NullDatum.get();
  }

  @Override
  public char[] getUnicodeChars(int fieldId) {
    return NullDatum.get().asUnicodeChars();
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

  @Override
  public Datum[] getValues() {
    return EMPTY_VALUES;
  }
}
