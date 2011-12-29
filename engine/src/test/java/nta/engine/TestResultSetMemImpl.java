package nta.engine;

import static org.junit.Assert.*;

import nta.catalog.Schema;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.junit.Before;
import org.junit.Test;

public class TestResultSetMemImpl {

	ResultSetMemImplOld res = null;
	
	@Before
	public void setUp() throws Exception {
		res = new ResultSetMemImplOld(new Schema());
		res.columnMap.put("id", 1);
		res.columnMap.put("age", 2);
		res.columnMap.put("name", 3);
		
		Tuple tuple = new VTuple(3);
		tuple.put(0, 1);
		tuple.put(1, 32);
		tuple.put(2, "hyunsik");		
		res.rows.add(tuple);
		
		tuple = new VTuple(3);
		tuple.put(0, 2);
		tuple.put(1, 27);
		tuple.put(2, "jimin");
		res.rows.add(tuple);
		
		tuple = new VTuple(3);
		tuple.put(0, 3);
		tuple.put(1, 50);
		tuple.put(2, "jone");
		res.rows.add(tuple);		
	}

	@Test
	public final void testNext() {
		res.next();
		assertEquals(res.getInt(0),1);
		assertEquals(res.getInt(1),32);
		assertEquals(res.getString(2),"hyunsik");
		
		res.next();
		assertEquals(res.getInt(0),2);
		assertEquals(res.getInt(1),27);
		assertEquals(res.getString(2),"jimin");
		
		res.next();
		assertEquals(res.getInt(0),3);
		assertEquals(res.getInt(1),50);
		assertEquals(res.getString(2),"jone");
		
		assertFalse(res.next());
	}

	@Test
	public final void testPrevious() {	
		res.last();
		
		res.previous();
		assertEquals(res.getInt(0),3);
		assertEquals(res.getInt(1),50);
		assertEquals(res.getString(2),"jone");
		
		res.previous();
		assertEquals(res.getInt(0),2);
		assertEquals(res.getInt(1),27);
		assertEquals(res.getString(2),"jimin");
		
		res.previous();
		assertEquals(res.getInt(0),1);
		assertEquals(res.getInt(1),32);
		assertEquals(res.getString(2),"hyunsik");		
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
