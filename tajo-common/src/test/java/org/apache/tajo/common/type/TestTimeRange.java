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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestTimeRange {
	TimeRange test1;
	TimeRange test2;
	
	@Before
	public void testSet() {
		test1 = new TimeRange(1321421413036l, 1321421442057l);
		test2 = new TimeRange(1321421580408l, 1321421592193l);
	}
	
	@Test
	public void testGet() {
		assertEquals(test1.getBegin(), 1321421413036l);
		assertEquals(test2.getEnd(), 1321421592193l);
	}

	@Test
	public void testCompare() {
		assertEquals(test2.compareTo(test1), 1321421580408l-1321421413036l);
	}
	
	@Test
	public void testEquals() {
		assertTrue(test1.equals(test1));
		assertFalse(test1.equals(100));
	}
	
//	@Test
//	public void testToString() {
//		assertEquals(test1.toString(), "(1321421413036l, 1321421442057l)");
//	}
}