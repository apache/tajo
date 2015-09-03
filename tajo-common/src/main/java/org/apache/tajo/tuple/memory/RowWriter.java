/***
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

package org.apache.tajo.tuple.memory;

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.storage.Tuple;

/**
 * The call sequence should be as follows:
 *
 * <pre>
 *   startRow() -->  skipField() or putXXX --> endRow()
 * </pre>
 *
 * The total number of skipField and putXXX invocations must be equivalent to the number of fields.
 */
public interface RowWriter {

  TajoDataTypes.DataType [] dataTypes();

  boolean startRow();

  void endRow();

  void skipField();

  void clear();

  void putByte(byte val);

  void putBool(boolean val);

  void putInt2(short val);

  void putInt4(int val);

  void putInt8(long val);

  void putFloat4(float val);

  void putFloat8(double val);

  void putText(String val);

  void putText(byte[] val);

  void putBlob(byte[] val);

  void putTimestamp(long val);

  void putTime(long val);

  void putDate(int val);

  void putInterval(IntervalDatum val);

  void putInet4(int val);

  void putProtoDatum(ProtobufDatum datum);

  void addTuple(Tuple tuple);
}
