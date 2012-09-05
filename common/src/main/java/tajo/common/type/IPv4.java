/*
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

package tajo.common.type;

import org.apache.hadoop.io.Writable;
import tajo.common.exception.InvalidAddressException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

public class IPv4 implements Writable, Comparable<IPv4> {
	
	public static IPv4 getSubnetMask(int len) {
		byte[] bytes = new byte[4];
		int index = 0;
		while (len > 7) {
			len -= 8;
			bytes[index++] = (byte)0xFF;
		}
		bytes[index] = (byte)((0xFF >> (8-len)) << (8-len));
		return new IPv4(bytes);
	}
	
	private byte[] ipBytes;
	
	public IPv4() {
		this.ipBytes = new byte[4];
	}
	
	public IPv4(byte[] bytes) {
		this.ipBytes = new byte[4];
		set(bytes);
	}
	
	public IPv4(String ipAddress) throws InvalidAddressException {
		this.ipBytes = new byte[4];
		this.set(ipAddress);
	}
	
	public void set(String ipAddress) throws InvalidAddressException {
		StringTokenizer tokenizer = new StringTokenizer(ipAddress);
		String token;
		for (int i = 0; i < 4; i++) {
			token = tokenizer.nextToken(".");
			if (token == null) {
				throw new InvalidAddressException();
			} else if (Integer.valueOf(token) < 0 || Integer.valueOf(token) > 255) {
				throw new InvalidAddressException();
			}
			//			ipBytes[i] = Short.valueOf(token).byteValue();
			this.ipBytes[i] = (byte)(((Integer.valueOf(token) << 24) >> 24) & 0xFF);
		}
	}
	
	public void set(byte[] bytes) {
		if (this.ipBytes == null) {
			this.ipBytes = new byte[4];
		}
		System.arraycopy(bytes, 0, this.ipBytes, 0, 4);
	}

	public byte[] getBytes() {
		return this.ipBytes;
	}
	
	/**
	 * This function will be provided as UDF later.
	 * @param addr
	 * @return
	 * @throws InvalidAddressException
	 */
	public boolean matchSubnet(String addr) throws InvalidAddressException {
		int maskIndex;
		if ((maskIndex=addr.indexOf('/')) != -1) {
			IPv4 other = new IPv4(addr.substring(0, maskIndex));
			int maskLen = Integer.valueOf(addr.substring(maskIndex+1));
			IPv4 subnetMask = IPv4.getSubnetMask(maskLen);
			if (this.and(subnetMask).equals(other.and(subnetMask))) {
				return true;
			} else {
				return false;
			}
		} else {
			throw new InvalidAddressException();
		}
	}
	
	/**
	 * This function will be provided as UDF later.
	 * @return
	 */
	public boolean matchGeoIP(/* country code */) {
		
		return false;
	}
	
	public IPv4 and(IPv4 other) {
		byte[] res = new byte[4];
		byte[] obytes = other.getBytes();
		
		for (int i = 0; i < 4; i++) {
			res[i] = (byte)(this.ipBytes[i] & obytes[i]);
		}
		
		return new IPv4(res);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof IPv4) {
			byte[] obytes = ((IPv4)o).getBytes();
			for (int i = 0; i < 4; i++) {
				if (this.ipBytes[i] != obytes[i]) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * This is a method for range query such as 'SELECT * FROM table WHERE srcIP > 163.152.23.0 and srcIP < 163.152.23.100'
	 */
	@Override
	public int compareTo(IPv4 o) {
		byte[] obytes = o.getBytes();
		for (int i = 0; i < 4; i++) {
			if (this.ipBytes[i] > obytes[i]) {
				return 1;
			} else if (this.ipBytes[i] < obytes[i]) {
				return -1;
			}
		}
		return 0;
	}
	
	@Override
	public String toString() {
		String str = "";
		int i;
		for (i = 0; i < 3; i++) {
			str += (((int)ipBytes[i] << 24) >> 24 & 0xFF) + ".";
		}
		str += (((int)ipBytes[i] << 24) >> 24 & 0xFF);
		return str;
	}
	
	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		in.readFully(this.ipBytes, 0, 4);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(this.ipBytes, 0, 4);
	}
}
