package nta.storage;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import nta.catalog.TableInfo;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestMemTupleSlots {
	TableInfo meta;
	Store store;
	MemTable slots;
	
	@Before
	public void setUp() throws Exception {
		meta = new TableInfo();
		store = new Store(new URI("mem://TestMemTupleSlot"), meta);
		slots = new MemTable(store);
		
		slots.addTuple(new VTuple(1));
		slots.addTuple(new VTuple(1));
		slots.addTuple(new VTuple(1));
		slots.addTuple(new VTuple(1));
		slots.addTuple(new VTuple(1));
	}

	@Test
	public final void testNextTuple() throws Exception {
		int cnt = 0;		
		while(slots.next() != null) {			
			cnt++;
		}
		assertEquals(cnt,5);
	}

	@Test
	public final void testReset() throws IOException, Exception {
		int cnt = 0;
		while(slots.next() != null) {
			cnt++;
		}
		slots.reset();
		while(slots.next() != null) {
			cnt++;
		}
		assertEquals(cnt,10);
	}

}
