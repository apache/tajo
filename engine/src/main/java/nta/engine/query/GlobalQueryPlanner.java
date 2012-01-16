package nta.engine.query;

/**
 * @author jihoon
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import nta.catalog.Catalog;
import nta.catalog.TabletServInfo;
import nta.engine.exec.SeqScanOp;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.FieldEval;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.parser.NQL;
import nta.engine.parser.RelInfo;
import nta.engine.parser.NQL.Query;
import nta.engine.plan.global.DecomposedQuery;
import nta.engine.plan.global.GenericTask;
import nta.engine.plan.global.GenericTaskTree;
import nta.engine.plan.logical.BinaryOp;
import nta.engine.plan.logical.LogicalOp;
import nta.engine.plan.logical.LogicalPlan;
import nta.engine.plan.logical.OpType;
import nta.engine.plan.logical.ProjectLO;
import nta.engine.plan.logical.RootOp;
import nta.engine.plan.logical.ScanOp;
import nta.engine.plan.logical.SelectionOp;
import nta.engine.plan.logical.UnaryOp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

public class GlobalQueryPlanner {
	private static Log LOG = LogFactory.getLog(GlobalQueryPlanner.class);
	
	private Catalog catalog;
	
	public GlobalQueryPlanner(Catalog catalog) throws IOException {
		this.catalog = catalog;
//		this.catalog.updateAllTabletServingInfo();
	}
	
	public GenericTaskTree buildPlan(LogicalPlan logicalPlan) throws IOException {
		LogicalPlan plan = logicalPlan;
		if (plan.getRoot().getType() != OpType.ROOT) {
			RootOp root = new RootOp();
			root.setSubOp(logicalPlan.getRoot());
			plan = new LogicalPlan(root);
		}
		GenericTaskTree localized = localize(plan);
		GenericTaskTree optimize = optimize(localized);
		return optimize;
	}
	
	public GenericTaskTree localize(LogicalPlan logicalPlan) throws IOException {
		// Build the generic task tree
		GenericTaskTree genericTree = buildGenericTaskTree(logicalPlan);
//		catalog.updateAllTabletServingInfo();
		
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
	
	public GenericTaskTree buildGenericTaskTree(LogicalPlan logicalPlan) {
		GenericTaskTree tree = new GenericTaskTree();
		GenericTask parent = null, child = null;
		LogicalOp op = logicalPlan.getRoot();
		ArrayList<GenericTask> q = new ArrayList<GenericTask>();
		parent = new GenericTask(op, null);
		tree.setRoot(parent);
		q.add(parent);

		// Depth-first traverse
		while (!q.isEmpty()) {
			parent = q.remove(0);
			op = parent.getOp();
			
			switch (op.getType()) {
			case ROOT:
				child = new GenericTask(((RootOp)op).getSubOp(), null);
				parent.addNextTask(child);
				child.addPrevTask(parent);
				q.add(child);
				break;
			case SCAN:
				// leaf
				break;
			case SELECTION:
			case PROJECTION:
				// intermediate, unary
				child = new GenericTask(((UnaryOp)op).getSubOp(), null);
				parent.addNextTask(child);
				child.addPrevTask(parent);
				q.add(child);
				break;
			case JOIN:
				// intermediate, binary
				child = new GenericTask(((BinaryOp)op).getInner(), null);
				parent.addNextTask(child);
				child.addPrevTask(parent);
				q.add(child);
				child = new GenericTask(((BinaryOp)op).getOuter(), null);
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
		LogicalOp op = task.getOp();
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
	
	public GenericTaskTree optimize(GenericTaskTree tree) {
		return tree;
	}
	
	public List<DecomposedQuery> decompose(GenericTaskTree tree) {
		Set<GenericTask> nextTaskSet;
		GenericTask task = tree.getRoot();
		ArrayList<DecomposedQuery> queryList = new ArrayList<DecomposedQuery>();
		List<TabletServInfo> tabletServInfoList;
		DecomposedQuery query;
		SubQueryRequest request;
		String host = null;
		int port = 0;
		LogicalPlan lplan = new LogicalPlan();
		ArrayList<GenericTask> s = new ArrayList<GenericTask>();
		// remove root operator
		nextTaskSet = task.getNextTasks();
		for (GenericTask t: nextTaskSet) {
			s.add(t);
		}
		
		while (!s.isEmpty()) {
			task = s.remove(0);
			if (lplan.getRoot() == null) {
				lplan.setRoot(task.getOp());
			}
			nextTaskSet = task.getNextTasks();
			
			// if an n-ary operator or a leaf node is visited, cut its all edges
			if ((nextTaskSet.size() > 1) ||
					nextTaskSet.size() == 0) {
				request = new SubQueryRequestImpl(task.getTablets(), new Path("hdfs://out/"+System.currentTimeMillis()).toUri(), 
						generateQuery(lplan), task.getTableName());
				tabletServInfoList = catalog.getHostByTable(task.getTableName());
				for (TabletServInfo servInfo : tabletServInfoList) {
					if (servInfo.getTablet().equals(request.getTablets().get(0))) {
						host = servInfo.getHostName();
						port = servInfo.getPort();
						break;
					}
				}
				query = new DecomposedQuery(request, port, host);
				queryList.add(query);
				lplan = new LogicalPlan();
			}
			
			for (GenericTask t: nextTaskSet) {
				s.add(t);
			}
		}
		
		return queryList;
	}
	
	public GenericTask[] localizeSimpleOp(GenericTask task) {
		ScanOp op = (ScanOp)task.getOp();
		List<TabletServInfo> tablets = catalog.getHostByTable(op.getRelName());
		GenericTask[] localized = new GenericTask[tablets.size()];
		TabletServInfo tablet;

		for (int i = 0; i < localized.length; i++) {
			// TODO: make tableInfo from tablets.get(i)
			localized[i] = new GenericTask();
			localized[i].setOp(task.getOp());
			localized[i].setAnnotation(task.getAnnotation());
			localized[i].setTableName(op.getRelName());
			
			// TODO: keep the alias
			// TODO: set startKey and endKey of TableInfo
			localized[i].addTablet(tablets.get(i).getTablet());
		}
		
		return localized;
	}
	
	private String selectHost(LogicalPlan plan) {
		// select the host which serves most tablets in the oplist
		return null;
	}
	
	public String generateQuery(LogicalPlan plan) {
		LogicalOp op = plan.getRoot();
		String strQuery = "";
		String from = "";
		String where = "";
		String proj = "*";
		ArrayList<LogicalOp> q = new ArrayList<LogicalOp>();
		q.add(op);
		
		while (!q.isEmpty()) {
			op = q.remove(0);
			switch (op.getType()) {
			case SCAN:
				ScanOp scan = (ScanOp)op;
				from = scan.getRelName();
				break;
			case SELECTION:
				SelectionOp sel = (SelectionOp)op;
				where = exprToString(sel.getQual());
				q.add(sel.getSubOp());
				break;
			case PROJECTION:
				ProjectLO projop = (ProjectLO)op;
				proj = "";
				for (TargetEntry te : projop.getTargetList()) {
					proj += te.colName + " ";
				}
				q.add(projop.getSubOp());
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
