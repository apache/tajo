/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.List;

import nta.distexec.DistPlan;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ScanNode;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author jihoon
 *
 */
public class TestGlobalOptimizer {
	GlobalOptimizer optimizer;
	QueryUnitGraph queryGraph;
	
	@Before
	public void setup() {
		optimizer = new GlobalOptimizer();
		String[] plans = {"merge", "local"};
		int[] nodeNum = {1, 3};
		MappingType[] mappings = {MappingType.ONE_TO_ONE, MappingType.ONE_TO_ONE};
		OptimizationPlan plan = new OptimizationPlan(2, plans, nodeNum, mappings);
		optimizer.addOptimizationPlan(ExprType.GROUP_BY, plan);
		
		QueryUnit root = new QueryUnit();
		root.set(new LogicalRootNode(), null);
		QueryUnit groupby = new QueryUnit();
		groupby.set(new GroupbyNode(null), null);
		root.addNextQuery(groupby);
		groupby.addPrevQuery(root);
		QueryUnit[] scans = new QueryUnit[5];
		for (int i = 0; i < 5; i++) {
			scans[i] = new QueryUnit();
			scans[i].set(new ScanNode(null), null);
			groupby.addNextQuery(scans[i]);
			scans[i].addPrevQuery(groupby);
		}
		
		queryGraph = new QueryUnitGraph(root);
	}

	@Test
	public void test() {
		QueryUnitGraph optimizedGraph = optimizer.optimize(queryGraph);
		List<QueryUnit> q = new ArrayList<QueryUnit>();
		QueryUnit query = optimizedGraph.getRoot();
		assertEquals(ExprType.ROOT, query.getOp().getType());
		assertEquals(1, query.getNextQueries().size());
		
		query = query.getNextQueries().iterator().next();
		assertEquals(ExprType.GROUP_BY, query.getOp().getType());
		DistPlan plan = query.getDistPlan();
		assertEquals("merge", plan.getPlanName());
		assertEquals(1, plan.getOutputNum());
		assertEquals(3, query.getNextQueries().size());
		
		for (QueryUnit uq : query.getNextQueries()) {
			assertEquals(ExprType.GROUP_BY, uq.getOp().getType());
			plan = uq.getDistPlan();
			assertEquals("local", plan.getPlanName());
			assertEquals(1, plan.getOutputNum());
			assertTrue(uq.getNextQueries().size() > 0);
			
			for (QueryUnit scan: uq.getNextQueries()) {
				assertEquals(ExprType.SCAN, scan.getOp().getType());
			}
		}
	}
}
