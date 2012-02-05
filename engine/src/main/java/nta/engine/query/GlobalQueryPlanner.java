package nta.engine.query;

/**
 * @author jihoon
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import nta.catalog.CatalogService;
import nta.catalog.HostInfo;
import nta.engine.planner.global.GlobalOptimizer;
import nta.engine.planner.global.GlobalQueryPlan;
import nta.engine.planner.global.MappingType;
import nta.engine.planner.global.OptimizationPlan;
import nta.engine.planner.global.QueryStep;
import nta.engine.planner.global.UnitQuery;
import nta.engine.planner.global.UnitQueryGraph;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.UnaryNode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GlobalQueryPlanner {
	private static Log LOG = LogFactory.getLog(GlobalQueryPlanner.class);
	
	private GlobalOptimizer optimizer;
	private CatalogService catalog;
	
	public GlobalQueryPlanner(CatalogService catalog) throws IOException {
		this.catalog = catalog;
		this.optimizer = new GlobalOptimizer();
		
		String[] plans = {"merge", "local"};
		int[] nodeNum = {1, 1};
		MappingType[] mappings = {MappingType.ONE_TO_ONE, MappingType.ONE_TO_ONE};
		OptimizationPlan plan = new OptimizationPlan(2, plans, nodeNum, mappings);
		this.optimizer.addOptimizationPlan(ExprType.GROUP_BY, plan);
	}
	
	public GlobalQueryPlan build(LogicalNode logicalPlan) throws IOException {
		UnitQueryGraph localized = localize(logicalPlan);
		UnitQueryGraph optimized = optimize(localized);
		GlobalQueryPlan plan = breakup(optimized);
		return plan;
	}
	
	private UnitQueryGraph localize(LogicalNode logicalPlan) throws IOException {
		// add union if necessary

		// Build the unit query graph
		UnitQueryGraph queryGraph = buildUnitQueryGraph(logicalPlan);
		UnitQuery query = queryGraph.getRoot();
		
		// For each level, localize each task
		ArrayList<UnitQuery> q = new ArrayList<UnitQuery>();
		q.add(query);
		
		while (!q.isEmpty()) {
			query = q.remove(0);

			localizeQuery(query);
			for (UnitQuery uq: query.getNextQueries()) {
				q.add(uq);
			}
		}
		return queryGraph;
	}
	
	private UnitQueryGraph buildUnitQueryGraph(LogicalNode logicalPlan) {
		UnitQuery parent = null, child = null;
		LogicalNode op = logicalPlan;
		ArrayList<UnitQuery> q = new ArrayList<UnitQuery>();
		parent = new UnitQuery(op);
		UnitQueryGraph graph = new UnitQueryGraph(parent);
		q.add(parent);

		// Depth-first traverse
		while (!q.isEmpty()) {
			parent = q.remove(0);
			op = parent.getOp();
			
			switch (op.getType()) {
			case ROOT:
				LogicalRootNode root = (LogicalRootNode)op;
				child = new UnitQuery(root.getSubNode());
				parent.addNextQuery(child);
				child.addPrevQuery(parent);
				q.add(child);
				break;
			case SCAN:
				// leaf
				ScanNode scan = (ScanNode)op;
				parent.setTableName(scan.getTableId());
				break;
			case SELECTION:
			case PROJECTION:
				// intermediate, unary
				child = new UnitQuery(((UnaryNode)op).getSubNode());
				parent.addNextQuery(child);
				child.addPrevQuery(parent);
				q.add(child);
				break;
			case JOIN:
				// intermediate, binary
				child = new UnitQuery(((BinaryNode)op).getLeftSubNode());
				parent.addNextQuery(child);
				child.addPrevQuery(parent);
				q.add(child);
				child = new UnitQuery(((BinaryNode)op).getRightSubNode());
				parent.addNextQuery(child);
				child.addPrevQuery(parent);
				q.add(child);
				break;
			case GROUP_BY:
				child = new UnitQuery(((UnaryNode)op).getSubNode());
				parent.addNextQuery(child);
				child.addPrevQuery(parent);
				q.add(child);
				break;
			case SORT:
				child = new UnitQuery(((UnaryNode)op).getSubNode());
				parent.addNextQuery(child);
				child.addPrevQuery(parent);
				q.add(child);
				break;
			case SET_UNION:
				break;
			case SET_DIFF:
				break;
			case SET_INTERSECT:
				break;
			case STORE:
				child = new UnitQuery(((UnaryNode)op).getSubNode());
				parent.addNextQuery(child);
				child.addPrevQuery(parent);
				q.add(child);
				break;
			}
			
		}
		return graph;
	}
	
	private void localizeQuery(UnitQuery query) {
		LogicalNode op = query.getOp();
		UnitQuery[] localizedQueries;
		Set<UnitQuery> prevQuerySet = query.getPrevQueries();
		Set<UnitQuery> nextQuerySet = query.getNextQueries();
		
		switch (op.getType()) {
		case SCAN:
		case SELECTION:
		case PROJECTION:
			localizedQueries = localizeSimpleQuery(query);
			// if prev exist, it is still not be localized
			if (prevQuerySet.size() > 0) {
				UnitQuery prev = prevQuerySet.iterator().next();
				prev.removeNextQuery(query);
				for (UnitQuery localize : localizedQueries) {
					prev.addNextQuery(localize);
				}
			}
			
			// if next exist..?
			
//			for (UnitQuery next: nextQuerySet) {
//				next.removePrevQuery(query);
//				for (UnitQuery localize: localizedQueries) {
//					next.addPrevQuery(localize);
//				}
//			}
			break;
		case JOIN:
			break;
		case GROUP_BY:
			break;
		case SORT:
			break;
		case SET_UNION:
			break;
		case SET_DIFF:
			break;
		case SET_INTERSECT:
			break;
		}
	}
	
	private UnitQueryGraph optimize(UnitQueryGraph graph) {
		return optimizer.optimize(graph);
	}
	
	class LevelLabeledUnitQuery {
		int level;
		UnitQuery query;
		
		public LevelLabeledUnitQuery(int level, UnitQuery query) {
			this.level = level;
			this.query = query;
		}
	}
	
	private GlobalQueryPlan breakup(UnitQueryGraph graph) {
		Set<UnitQuery> nextQuerySet;
		LevelLabeledUnitQuery e;
		int curLevel = 0;
		GlobalQueryPlan globalPlan = new GlobalQueryPlan();
		ArrayList<LevelLabeledUnitQuery> s = new ArrayList<GlobalQueryPlanner.LevelLabeledUnitQuery>();
		QueryStep queryStep = new QueryStep();
		s.add(new LevelLabeledUnitQuery(0, graph.getRoot()));
		
		while (!s.isEmpty()) {
			e = s.remove(0);
			nextQuerySet = e.query.getNextQueries();
			if (e.query.getOp().getType() != ExprType.ROOT) {
				// remove root operator

				if (curLevel != e.level) {
					globalPlan.addQueryStep(queryStep);
					queryStep = new QueryStep();
					curLevel = e.level;
				}

				// if an n-ary node or a leaf node is visited, break up the graph
//				if ((nextQuerySet.size() > 1) ||
//						nextQuerySet.size() == 0) {
					queryStep.addQuery(e.query);
//				} else {
//
//				}
			}
			nextQuerySet = e.query.getNextQueries();
			for (UnitQuery t: nextQuerySet) {
				s.add(new LevelLabeledUnitQuery(e.level+1, t));
			}
		}
		
		if (queryStep.size() > 0) {
			globalPlan.addQueryStep(queryStep);
		}
		
		return globalPlan;
	}
	
	private UnitQuery[] localizeSimpleQuery(UnitQuery query) {
		ScanNode op = (ScanNode)query.getOp();
		List<HostInfo> fragments = catalog.getHostByTable(op.getTableId());
		UnitQuery[] localized = new UnitQuery[fragments.size()];

		for (int i = 0; i < localized.length; i++) {
			// TODO: make tableInfo from tablets.get(i)
			localized[i] = new UnitQuery(query.getOp(), query.getAnnotation());
			localized[i].setTableName(op.getTableId());
			
			// TODO: keep the alias
			// TODO: set startKey and endKey of TableInfo
			localized[i].addFragment(fragments.get(i).getFragment());
		}
		
		return localized;
	}
	
	private UnitQuery[] localizeComplexQuery(UnitQuery query) {
		Set<UnitQuery> nextQuerySet = query.getNextQueries();
		// TODO: localize시킬 unit query의 수를 결정하는 것이 필요
		// TODO: 한 스텝으로 끝나지 않을 수도 있음
		UnitQuery[] localized = new UnitQuery[nextQuerySet.size()];
		for (int i = 0; i < localized.length; i++) {
			localized[i] = new UnitQuery(query.getOp(), query.getAnnotation());
		}
		return localized;
	}
	
	private String selectHost(LogicalNode plan) {
		// select the host which serves most tablets in the oplist
		return null;
	}
	
}
