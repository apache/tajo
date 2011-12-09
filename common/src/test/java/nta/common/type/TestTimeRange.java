package nta.common.type;

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