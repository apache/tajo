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

package org.apache.tajo.tuple;

import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.storage.offheap.HeapTuple;
import org.apache.tajo.storage.offheap.RowWriter;
import org.apache.tajo.storage.offheap.ZeroCopyTuple;

public interface TupleBuilder {
  public boolean startRow();

  public TupleBuilder endRow();

  public HeapTuple asHeapTuple();

  public ZeroCopyTuple asZeroCopyTuple();

  public void skipField();

  public void putBool(boolean val);

  public void putInt2(short val);

  public void putInt4(int val);

  public void putInt8(long val);

  public void putFloat4(float val);

  public void putFloat8(double val);

  public void putText(String val);

  public void putText(byte [] val);

  public void putBlob(byte[] val);

  public void putTimestamp(long val);

  public void putDate(int val);

  public void putTime(long val);

  public void putInterval(IntervalDatum val);

  public void putInet4(int val);
}
