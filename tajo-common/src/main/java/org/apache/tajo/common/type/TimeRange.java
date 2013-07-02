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

package org.apache.tajo.common.type;

import java.nio.ByteBuffer;

public class TimeRange implements Comparable<TimeRange>{

	private long begin;
	private long end;
	
	public TimeRange() {
		
	}
	
	public TimeRange(long begin, long end) {
		this.set(begin, end);
	}
	
	public void set(long begin, long end) {
		this.begin = begin;
		this.end = end;
	}
	
	public long getBegin() {
		return this.begin;
	}
	
	public long getEnd() {
		return this.end;
	}

	/*
	 * TimeRange must not be overlapped with other TimeRange.
	 */
	@Override
	public int compareTo(TimeRange o) {
		return (int)(this.begin - o.getBegin());
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof TimeRange) {
			TimeRange tr = (TimeRange)o;
			if (this.begin == tr.getBegin()) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		byte[] array = new byte[8];
		ByteBuffer bb = ByteBuffer.wrap(array);
		bb.putLong(this.begin);
		return bb.hashCode();
	}
	
	@Override
	public String toString() {
		return new String("(" + begin + ", " + end + ")");
	}
}
