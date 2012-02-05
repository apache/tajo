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
	UnitQueryGraph queryGraph;
	
	@Before
	public void setup() {
		optimizer = new GlobalOptimizer();
		String[] plans = {"merge", "local"};
		int[] nodeNum = {1, 3};
		MappingType[] mappings = {MappingType.ONE_TO_ONE, MappingType.ONE_TO_ONE};
		OptimizationPlan plan = new OptimizationPlan(2, plans, nodeNum, mappings);
		optimizer.addOptimizationPlan(ExprType.GROUP_BY, plan);
		
		UnitQuery root = new UnitQuery();
		root.set(new LogicalRootNode(), null);
		UnitQuery groupby = new UnitQuery();
		groupby.set(new GroupbyNode(null), null);
		root.addNextQuery(groupby);
		groupby.addPrevQuery(root);
		UnitQuery[] scans = new UnitQuery[5];
		for (int i = 0; i < 5; i++) {
			scans[i] = new UnitQuery();
			scans[i].set(new ScanNode(null), null);
			groupby.addNextQuery(scans[i]);
			scans[i].addPrevQuery(groupby);
		}
		
		queryGraph = new UnitQueryGraph(root);
	}

	@Test
	public void test() {
		UnitQueryGraph optimizedGraph = optimizer.optimize(queryGraph);
		List<UnitQuery> q = new ArrayList<UnitQuery>();
		UnitQuery query = optimizedGraph.getRoot();
		assertEquals(ExprType.ROOT, query.getOp().getType());
		assertEquals(1, query.getNextQueries().size());
		
		query = query.getNextQueries().iterator().next();
		assertEquals(ExprType.GROUP_BY, query.getOp().getType());
		DistPlan plan = query.getDistPlan();
		assertEquals("merge", plan.getPlanName());
		assertEquals(1, plan.getOutputNum());
		assertEquals(3, query.getNextQueries().size());
		
		for (UnitQuery uq : query.getNextQueries()) {
			assertEquals(ExprType.GROUP_BY, uq.getOp().getType());
			plan = uq.getDistPlan();
			assertEquals("local", plan.getPlanName());
			assertEquals(1, plan.getOutputNum());
			assertTrue(uq.getNextQueries().size() > 0);
			
			for (UnitQuery scan: uq.getNextQueries()) {
				assertEquals(ExprType.SCAN, scan.getOp().getType());
			}
		}
	}
}
