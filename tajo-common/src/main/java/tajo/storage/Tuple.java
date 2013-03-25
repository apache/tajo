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

package tajo.storage;

import tajo.datum.*;

import java.net.InetAddress;

// TODO - to be unified in tajo-core-storage
public interface Tuple {
  
	public int size();
	
	public boolean contains(int fieldid);

  public boolean isNull(int fieldid);
	
	public void clear();
	
	public void put(int fieldId, Datum value);

  public void put(int fieldId, Datum[] values);

  public void put(int fieldId, Tuple tuple);
	
	public void put(Datum[] values);
	
	public Datum get(int fieldId);
	
	public void setOffset(long offset);
	
	public long getOffset();

	public BoolDatum getBoolean(int fieldId);
	
	public ByteDatum getByte(int fieldId);

  public CharDatum getChar(int fieldId);
	
	public BytesDatum getBytes(int fieldId);
	
	public ShortDatum getShort(int fieldId);
	
	public IntDatum getInt(int fieldId);
	
	public LongDatum getLong(int fieldId);
	
	public FloatDatum getFloat(int fieldId);
	
	public DoubleDatum getDouble(int fieldId);
	
	public IPv4Datum getIPv4(int fieldId);
	
	public byte [] getIPv4Bytes(int fieldId);
	
	public InetAddress getIPv6(int fieldId);
	
	public byte [] getIPv6Bytes(int fieldId);
	
	public StringDatum getString(int fieldId);

  public StringDatum2 getString2(int fieldId);
}
