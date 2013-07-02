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

import org.apache.tajo.datum.*;

import java.net.InetAddress;

public interface Tuple {

  public int size();

  public boolean contains(int fieldid);

  public boolean isNull(int fieldid);

  public void clear();

  public void put(int fieldId, Datum value);

  public void put(int fieldId, Datum [] values);

  public void put(int fieldId, Tuple tuple);

  public void put(Datum [] values);

  public Datum get(int fieldId);

  public void setOffset(long offset);

  public long getOffset();

  public BooleanDatum getBoolean(int fieldId);

  public BitDatum getByte(int fieldId);

  public CharDatum getChar(int fieldId);

  public BlobDatum getBytes(int fieldId);

  public Int2Datum getShort(int fieldId);

  public Int4Datum getInt(int fieldId);

  public Int8Datum getLong(int fieldId);

  public Float4Datum getFloat(int fieldId);

  public Float8Datum getDouble(int fieldId);

  public Inet4Datum getIPv4(int fieldId);

  public byte [] getIPv4Bytes(int fieldId);

  public InetAddress getIPv6(int fieldId);

  public byte [] getIPv6Bytes(int fieldId);

  public TextDatum getString(int fieldId);

  public TextDatum getText(int fieldId);
}
