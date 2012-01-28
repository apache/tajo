/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.List;

import nta.engine.ipc.protocolrecords.Fragment;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author jihoon
 *
 */
public class TestUnitQuery {

	@Test
	public void testCreate() {
		UnitQuery q1 = new UnitQuery();
		UnitQuery q2 = new UnitQuery();
		UnitQuery q3 = new UnitQuery();
		
		assertEquals(q1.getId()+1, q2.getId());
		assertEquals(q2.getId()+1, q3.getId());
	}
}