/**
 * 
 */
package nta.storage;


import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Hyunsik Choi
 *
 */
public class TestVTuple {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		
	}
	
	@Test
	public void testContain() {
		VTuple t1 = new VTuple(260);
		t1.put(0, 1);
		t1.put(1, 1);
		t1.put(27, 1);
		t1.put(96, 1);
		t1.put(257, 1);
		
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
		System.out.println(t1);
	}
	
	@Test
	public void testPut() {
		VTuple t1 = new VTuple(260);
		t1.put(0, "str");
		t1.put(1, 2);
		t1.put(257, 0.76f);
		
		assertTrue(t1.contains(0));
		assertTrue(t1.contains(1));
		
		assertEquals(t1.getString(0),"str");
		assertEquals(t1.getInt(1),2);
		assertTrue(t1.getFloat(257) == 0.76f);
		
	}
}
