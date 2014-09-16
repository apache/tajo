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
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;

public interface Tuple extends Cloneable {
  
	public int size();
	
	public boolean contains(int fieldid);

  public boolean isNull(int fieldid);

  public boolean isNotNull(int fieldid);
	
	public void clear();
	
	public void put(int fieldId, Datum value);

  public void put(int fieldId, Datum[] values);

  public void put(int fieldId, Tuple tuple);
	
	public void put(Datum[] values);
	
	public Datum get(int fieldId);
	
	public void setOffset(long offset);
	
	public long getOffset();

  @SuppressWarnings("unused")
	public boolean getBool(int fieldId);

  @SuppressWarnings("unused")
	public byte getByte(int fieldId);

  @SuppressWarnings("unused")
  public char getChar(int fieldId);

	public byte [] getBytes(int fieldId);

  @SuppressWarnings("unused")
	public short getInt2(int fieldId);

  @SuppressWarnings("unused")
	public int getInt4(int fieldId);

  @SuppressWarnings("unused")
	public long getInt8(int fieldId);

  @SuppressWarnings("unused")
	public float getFloat4(int fieldId);

  @SuppressWarnings("unused")
	public double getFloat8(int fieldId);

  @SuppressWarnings("unused")
	public String getText(int fieldId);

  public IntervalDatum getInterval(int fieldId);

  @SuppressWarnings("unused")
  public ProtobufDatum getProtobufDatum(int field);

  public Tuple clone() throws CloneNotSupportedException;

  public Datum[] getValues();
}
