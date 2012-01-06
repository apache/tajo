package nta.storage;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.StoreType;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestMemTupleSlots {
	TableMeta meta;
	MemTable slots;
	
	@Before
	public void setUp() throws Exception {	  
	  meta = new TableMetaImpl(new Schema(), StoreType.MEM);	  
		slots = new MemTable(meta, URI.create("mem://memtable1"));
		
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
