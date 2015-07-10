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


import org.apache.tajo.datum.DatumFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestVTuple {

	/**
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		
	}
	
	@Test
	public void testContain() {
		VTuple t1 = new VTuple(260);
		t1.put(0, DatumFactory.createInt4(1));
		t1.put(1, DatumFactory.createInt4(1));
		t1.put(27, DatumFactory.createInt4(1));
		t1.put(96, DatumFactory.createInt4(1));
		t1.put(257, DatumFactory.createInt4(1));
		
		assertTrue(t1.contains(0));
		assertTrue(t1.contains(1));
		assertFalse(t1.contains(2));
		assertFalse(t1.contains(3));
		assertFalse(t1.contains(4));
		assertTrue(t1.contains(27));
		assertFalse(t1.contains(28));
		assertFalse(t1.contains(95));
		assertTrue(t1.contains(96));
		assertFalse(t1.contains(97));
		assertTrue(t1.contains(257));
	}
	
	@Test
	public void testPut() {
		VTuple t1 = new VTuple(260);
		t1.put(0, DatumFactory.createText("str"));
		t1.put(1, DatumFactory.createInt4(2));
		t1.put(257, DatumFactory.createFloat4(0.76f));
		
		assertTrue(t1.contains(0));
		assertTrue(t1.contains(1));
		
		assertEquals(t1.getText(0),"str");
		assertEquals(t1.get(1).asInt4(),2);
		assertTrue(t1.get(257).asFloat4() == 0.76f);
	}

  @Test
	public void testEquals() {
	  Tuple t1 = new VTuple(5);
	  Tuple t2 = new VTuple(5);
	  
	  t1.put(0, DatumFactory.createInt4(1));
	  t1.put(1, DatumFactory.createInt4(2));
	  t1.put(3, DatumFactory.createInt4(2));
	  
	  t2.put(0, DatumFactory.createInt4(1));
    t2.put(1, DatumFactory.createInt4(2));
    t2.put(3, DatumFactory.createInt4(2));
    
    assertEquals(t1,t2);
    
    Tuple t3 = new VTuple(5);
    t2.put(0, DatumFactory.createInt4(1));
    t2.put(1, DatumFactory.createInt4(2));
    t2.put(4, DatumFactory.createInt4(2));
    
    assertNotSame(t1,t3);
	}
	
	@Test
	public void testHashCode() {
	  Tuple t1 = new VTuple(5);
    Tuple t2 = new VTuple(5);
    
    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(3, DatumFactory.createInt4(2));
    t1.put(4, DatumFactory.createText("hyunsik"));
    
    t2.put(0, DatumFactory.createInt4(1));
    t2.put(1, DatumFactory.createInt4(2));
    t2.put(3, DatumFactory.createInt4(2));
    t2.put(4, DatumFactory.createText("hyunsik"));
    
    assertEquals(t1.hashCode(),t2.hashCode());
    
    Tuple t3 = new VTuple(5);
    t3.put(0, DatumFactory.createInt4(1));
    t3.put(1, DatumFactory.createInt4(2));
    t3.put(4, DatumFactory.createInt4(2));
    
    assertNotSame(t1.hashCode(),t3.hashCode());
	}

  @Test
  public void testClone() throws CloneNotSupportedException {
    VTuple t1 = new VTuple(5);

    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));
    t1.put(3, DatumFactory.createInt4(2));
    t1.put(4, DatumFactory.createText("str"));

    VTuple t2 = t1.clone();
    assertNotSame(t1, t2);
    assertEquals(t1, t2);

    assertSame(t1.get(4), t2.get(4));

    t1.clear();
    assertFalse(t1.equals(t2));
  }
}
