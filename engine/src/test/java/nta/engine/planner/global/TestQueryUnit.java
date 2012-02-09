/**
 * 
 */
package nta.engine.planner.global;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author jihoon
 *
 */
public class TestQueryUnit {

	@Test
	public void testCreate() {
		QueryUnit q1 = new QueryUnit();
		QueryUnit q2 = new QueryUnit();
		QueryUnit q3 = new QueryUnit();
		
		assertEquals(q1.getId()+1, q2.getId());
		assertEquals(q2.getId()+1, q3.getId());
	}
}