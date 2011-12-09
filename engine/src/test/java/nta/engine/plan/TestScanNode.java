package nta.engine.plan;

import static org.junit.Assert.*;

import nta.engine.plan.NodeType;
import nta.engine.plan.ScanNode;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestScanNode {
	public ScanNode node;
	
	@Before
	public void setUp() throws Exception {
		node = new ScanNode();
	}

	@Test
	public final void testGetType() {
		assertEquals(node.getType(),NodeType.Scan);
	}

}
