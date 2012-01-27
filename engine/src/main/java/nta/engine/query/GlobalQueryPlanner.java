package nta.engine.query;

/**
 * @author jihoon
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nta.catalog.CatalogServer;
import nta.catalog.TabletServInfo;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.FieldEval;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.plan.global.DecomposedQuery;
import nta.engine.plan.global.GenericTask;
import nta.engine.plan.global.GenericTaskGraph;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.UnaryNode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

public class GlobalQueryPlanner {
	private static Log LOG = LogFactory.getLog(GlobalQueryPlanner.class);
	
	private CatalogServer catalog;
	
	public GlobalQueryPlanner(CatalogServer catalog) throws IOException {
		this.catalog = catalog;
	}
	
	public GenericTaskGraph buildPlan(LogicalNode logicalPlan) throws IOException {
		LogicalNode plan = logicalPlan;
		// add union
		GenericTaskGraph localized = localize(plan);
		GenericTaskGraph optimize = optimize(localized);
		return optimize;
	}
	
	public GenericTaskGraph localize(LogicalNode logicalPlan) throws IOException {
		// Build the generic task tree
		GenericTaskGraph genericTree = buildGenericTaskGraph(logicalPlan);
		
		// For each level, localize each task
		GenericTask task = genericTree.getRoot();
		ArrayList<GenericTask> q = new ArrayList<GenericTask>();
		q.add(task);
		
		while (!q.isEmpty()) {
			task = q.remove(0);
			localizeTask(task);
			for (GenericTask t: task.getNextTasks()) {
				q.add(t);
			}
		}
		return genericTree;
	}
	
	public GenericTaskGraph buildGenericTaskGraph(LogicalNode logicalPlan) {
		GenericTaskGraph tree = new GenericTaskGraph();
		GenericTask parent = null, child = null;
		LogicalNode op = logicalPlan;
		ArrayList<GenericTask> q = new ArrayList<GenericTask>();
		parent = new GenericTask(op, null);
		tree.setRoot(parent);
		q.add(parent);

		// Depth-first traverse
		while (!q.isEmpty()) {
			parent = q.remove(0);
			op = parent.getOp();
			
			switch (op.getType()) {
			case SCAN:
				// leaf
				break;
			case SELECTION:
			case PROJECTION:
				// intermediate, unary
				child = new GenericTask(((UnaryNode)op).getSubNode(), null);
				parent.addNextTask(child);
				child.addPrevTask(parent);
				q.add(child);
				break;
			case JOIN:
				// intermediate, binary
				child = new GenericTask(((BinaryNode)op).getLeftSubNode(), null);
				parent.addNextTask(child);
				child.addPrevTask(parent);
				q.add(child);
				child = new GenericTask(((BinaryNode)op).getRightSubNode(), null);
				parent.addNextTask(child);
				child.addPrevTask(parent);
				q.add(child);
				break;
			case GROUP_BY:
				break;
			case RENAME:
				break;
			case SORT:
				break;
			case SET_UNION:
				break;
			case SET_DIFF:
				break;
			case SET_INTERSECT:
				break;
			case INSERT_INTO:
				break;
			case CREATE_TABLE:
			case SHOW_TABLE:
			case DESC_TABLE:
			case SHOW_FUNCTION:
				// leaf
				break;
			}
			
		}
		return tree;
	}
	
	public void localizeTask(GenericTask task) {
		LogicalNode op = task.getOp();
		GenericTask[] localizedTasks;
		Set<GenericTask> prevTaskSet = task.getPrevTasks();
		Set<GenericTask> nextTaskSet = task.getNextTasks();
		
		switch (op.getType()) {
		case ROOT:
			break;
		case SCAN:
		case SELECTION:
		case PROJECTION:
			localizedTasks = localizeSimpleOp(task);
			for (GenericTask prev: prevTaskSet) {
				prev.removeNextTask(task);
				for (GenericTask localize: localizedTasks) {
					prev.addNextTask(localize);
				}
			}
			for (GenericTask next: nextTaskSet) {
				next.removePrevTask(task);
				for (GenericTask localize: localizedTasks) {
					next.addPrevTask(localize);
				}
			}
			break;
		case JOIN:
			break;
		case GROUP_BY:
			break;
		case RENAME:
			break;
		case SORT:
			break;
		case SET_UNION:
			break;
		case SET_DIFF:
			break;
		case SET_INTERSECT:
			break;
		case INSERT_INTO:
			break;
		case CREATE_TABLE:
			break;
		case SHOW_TABLE:
			break;
		case DESC_TABLE:
			break;
		case SHOW_FUNCTION:
			break;
		}
	}
	
	public GenericTaskGraph optimize(GenericTaskGraph tree) {
		return tree;
	}
	
	public List<DecomposedQuery> decompose(GenericTaskGraph tree) {
		Set<GenericTask> nextTaskSet;
		GenericTask task = tree.getRoot();
		ArrayList<DecomposedQuery> queryList = new ArrayList<DecomposedQuery>();
		List<TabletServInfo> tabletServInfoList;
		DecomposedQuery query;
		SubQueryRequest request;
		String host = null;
		int port = 0;
		ArrayList<GenericTask> s = new ArrayList<GenericTask>();
		// remove root operator
		nextTaskSet = task.getNextTasks();
		for (GenericTask t: nextTaskSet) {
			s.add(t);
		}
		
		String strQuery = "";
		while (!s.isEmpty()) {
			task = s.remove(0);
			nextTaskSet = task.getNextTasks();
			
			// if an n-ary operator or a leaf node is visited, cut its all edges
			if ((nextTaskSet.size() > 1) ||
					nextTaskSet.size() == 0) {
				strQuery = generateQuery(task.getOp());
				tabletServInfoList = catalog.getHostByTable(task.getTableName());
				for (Fragment t : task.getFragments()) {
					List<Fragment> tablets = new ArrayList<Fragment>();
					tablets.add(t);
					request = new SubQueryRequestImpl(0, tablets, new Path("hdfs://out/"+System.currentTimeMillis()).toUri(), 
							strQuery, t.getId());
					for (TabletServInfo servInfo : tabletServInfoList) {
						if (servInfo.getTablet().equals(request.getFragments().get(0))) {
							host = servInfo.getHostName();
							port = servInfo.getPort();
							break;
						}
					}
					query = new DecomposedQuery(request, port, host);
					queryList.add(query);
				}
//				request = new SubQueryRequestImpl(task.getTablets(), new Path("hdfs://out/"+System.currentTimeMillis()).toUri(), 
//						generateQuery(lplan), task.getTableName());
//				for (TabletServInfo servInfo : tabletServInfoList) {
//					if (servInfo.getTablet().equals(request.getTablets().get(0))) {
//						host = servInfo.getHostName();
//						port = servInfo.getPort();
//						break;
//					}
//				}
//				query = new DecomposedQuery(request, port, host);
//				queryList.add(query);
			}
			
			for (GenericTask t: nextTaskSet) {
				s.add(t);
			}
		}
		
		return queryList;
	}
	
	public GenericTask[] localizeSimpleOp(GenericTask task) {
		ScanNode op = (ScanNode)task.getOp();
		List<TabletServInfo> tablets = catalog.getHostByTable(op.getTableId());
		GenericTask[] localized = new GenericTask[tablets.size()];

		for (int i = 0; i < localized.length; i++) {
			// TODO: make tableInfo from tablets.get(i)
			localized[i] = new GenericTask();
			localized[i].setOp(task.getOp());
			localized[i].setAnnotation(task.getAnnotation());
			localized[i].setTableName(op.getTableId());
			
			// TODO: keep the alias
			// TODO: set startKey and endKey of TableInfo
			localized[i].addFragment(tablets.get(i).getTablet());
		}
		
		return localized;
	}
	
	private String selectHost(LogicalNode plan) {
		// select the host which serves most tablets in the oplist
		return null;
	}
	
	public String generateQuery(LogicalNode plan) {
		LogicalNode op = plan;
		String strQuery = "";
		String from = "";
		String where = "";
		String proj = "*";
		ArrayList<LogicalNode> q = new ArrayList<LogicalNode>();
		q.add(op);
		
		while (!q.isEmpty()) {
			op = q.remove(0);
			switch (op.getType()) {
			case SCAN:
				ScanNode scan = (ScanNode)op;
				from = scan.getTableId();
				break;
			case SELECTION:
				SelectionNode sel = (SelectionNode)op;
				where = exprToString(sel.getQual());
				q.add(sel.getSubNode());
				break;
			case PROJECTION:
				ProjectionNode projop = (ProjectionNode)op;
				proj = "";
				for (Target te : projop.getTargetList()) {
					proj += te.getColumnSchema().getColumnName() + " ";
				}
				q.add(projop.getSubNode());
				break;
			default:
				break;
			}
		}
		
		strQuery = "select " + proj;
		if (!from.equals("")) {
			strQuery += " from " + from;
		}
		if (!where.equals("")) {
			strQuery += " where " + where;
		}
		return strQuery;
	}
	
	private String exprToString(EvalNode expr) {
		String str = "";
		ArrayList<EvalNode> s = new ArrayList<EvalNode>();
		HashSet<EvalNode> hs = new HashSet<EvalNode>();
		EvalNode e;
		s.add(expr);
		
		while (!s.isEmpty()) {
			e = s.remove(0);
			hs.add(e);
			if (e.getLeftExpr() == null && e.getRightExpr() == null) {
				// leaf
				str += getStringOfExpr(e) + " ";
			} else {
				if (e.getRightExpr() != null) {
					if (!hs.contains(e.getRightExpr())) {
						s.add(0, e.getRightExpr());
					}
				}
				s.add(e);
				if (e.getLeftExpr() != null) {
					if (!hs.contains(e.getLeftExpr())) {
						s.add(0, e.getLeftExpr());
					} else {
						// finished the left traverse
						str += getStringOfExpr(e) + " ";
					}
				}
			}
		}
		
		return str;
	}
	
	private String getStringOfExpr(EvalNode expr) {
		String ret = null;
		switch (expr.getType()) {
		case FIELD:
			FieldEval field = (FieldEval) expr;
			ret = field.getName();
			break;
		case FUNCTION:
			FuncCallEval func = (FuncCallEval) expr;
			ret = func.getName();
			break;
		case AND:
			ret = "AND";
			break;
		case OR:
			ret = "OR";
			break;
		case CONST:
			ConstEval con = (ConstEval) expr;
			ret = con.toString();
			break;
		case PLUS:
			ret = "+";
			break;
		case MINUS:
			ret = "-";
			break;
		case MULTIPLY:
			ret = "*";
			break;
		case DIVIDE:
			ret = "/";
			break;
		case EQUAL:
			ret = "=";
			break;
		case NOT_EQUAL:
			ret = "!=";
			break;
		case LTH:
			ret = "<";
			break;
		case LEQ:
			ret = "<=";
			break;
		case GTH:
			ret = ">";
			break;
		case GEQ:
			ret = ">=";
			break;
		}
		return ret;
	}
}
