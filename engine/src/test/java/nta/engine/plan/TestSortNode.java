package nta.engine.plan;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import nta.engine.plan.NodeType;
import nta.engine.plan.SortNode;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestSortNode {
	public SortNode node;
	List<Integer> sortFields;
	static String TableName = "L1Table";
	
	@Before
	public void setUp() throws Exception {
		node = new SortNode();
		sortFields = new ArrayList<Integer>();
		sortFields.add(1);
		sortFields.add(3);
		sortFields.add(5);
		node.setSortFields(sortFields);
		node.setDecendant();
	}

	@Test
	public final void testSetTableName() {		
		assertEquals(node.getSortFields(),sortFields);
	}

	@Test
	public final void testOrder() {
		assertFalse(node.getAscendant());		
		node.setAscendant();
		assertTrue(node.getAscendant());
	}

	@Test
	public final void testGetType() {
		assertEquals(node.getType(),NodeType.Sort);
	}

}
