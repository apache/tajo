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

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.util.datetime.TimeMeta;

public interface Tuple extends Cloneable {

  int size();

  boolean contains(int fieldid);

  boolean isBlank(int fieldid);

  boolean isBlankOrNull(int fieldid);

  void put(int fieldId, Tuple tuple);

  void put(int fieldId, Datum value);

  void put(Datum[] values);

  void clear();

  Datum asDatum(int fieldId);

  TajoDataTypes.Type type(int fieldId);

  int size(int fieldId);

  void setOffset(long offset);

  long getOffset();

  boolean getBool(int fieldId);

  byte getByte(int fieldId);

  char getChar(int fieldId);

  byte[] getBytes(int fieldId);

  byte[] getTextBytes(int fieldId);

  short getInt2(int fieldId);

  int getInt4(int fieldId);

  long getInt8(int fieldId);

  float getFloat4(int fieldId);

  double getFloat8(int fieldId);

  String getText(int fieldId);

  TimeMeta getTimeDate(int fieldId);

  Datum getProtobufDatum(int fieldId);

  Datum getInterval(int fieldId);

  // todo remove this
  char [] getUnicodeChars(int fieldId);

  Tuple clone() throws CloneNotSupportedException;

  // todo remove this
  Datum[] getValues();
}
