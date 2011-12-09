package nta.engine;

import static org.junit.Assert.*;

import nta.catalog.Schema;
import nta.storage.MemTuple;

import org.junit.Before;
import org.junit.Test;

public class TestResultSetMemImpl {

	ResultSetMemImpl res = null;
	
	@Before
	public void setUp() throws Exception {
		res = new ResultSetMemImpl(new Schema());
		res.columnMap.put("id", 1);
		res.columnMap.put("age", 2);
		res.columnMap.put("name", 3);
		
		MemTuple tuple = new MemTuple();
		tuple.putInt(1, 1);
		tuple.putInt(2, 32);
		tuple.putString(3, "hyunsik");		
		res.rows.add(tuple);
		
		tuple = new MemTuple();
		tuple.putInt(1, 2);
		tuple.putInt(2, 27);
		tuple.putString(3, "jimin");
		res.rows.add(tuple);
		
		tuple = new MemTuple();
		tuple.putInt(1, 3);
		tuple.putInt(2, 50);
		tuple.putString(3, "jone");
		res.rows.add(tuple);		
	}

	@Test
	public final void testNext() {
		res.next();
		assertEquals(res.getInt(1),1);
		assertEquals(res.getInt(2),32);
		assertEquals(res.getString(3),"hyunsik");
		
		res.next();
		assertEquals(res.getInt(1),2);
		assertEquals(res.getInt(2),27);
		assertEquals(res.getString(3),"jimin");
		
		res.next();
		assertEquals(res.getInt(1),3);
		assertEquals(res.getInt(2),50);
		assertEquals(res.getString(3),"jone");
		
		assertFalse(res.next());
	}

	@Test
	public final void testPrevious() {	
		res.last();
		
		res.previous();
		assertEquals(res.getInt(1),3);
		assertEquals(res.getInt(2),50);
		assertEquals(res.getString(3),"jone");
		
		res.previous();
		assertEquals(res.getInt(1),2);
		assertEquals(res.getInt(2),27);
		assertEquals(res.getString(3),"jimin");
		
		res.previous();
		assertEquals(res.getInt(1),1);
		assertEquals(res.getInt(2),32);
		assertEquals(res.getString(3),"hyunsik");		
	}

	@Test
	public final void testFirst() {
		res.next();
		res.next();
		res.next();
		
		res.first();
		assertTrue(res.next());
		assertTrue(res.next());
		assertTrue(res.next());
		assertFalse(res.next());
	}

	@Test
	public final void testLast() {		
		res.last();
		assertTrue(res.previous());
		assertTrue(res.previous());
		assertTrue(res.previous());
		assertFalse(res.previous());
	}

	@Test
	public final void testGetRow() {
		assertEquals(res.getRow(),-1);
	}

}
