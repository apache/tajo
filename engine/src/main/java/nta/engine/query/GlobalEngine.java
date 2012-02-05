/**
 * 
 */
package nta.engine.query;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.HostInfo;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.EngineService;
import nta.engine.QueryContext;
import nta.engine.ResultSetMemImplOld;
import nta.engine.ResultSetOld;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.PhysicalOp;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.FieldEval;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.ipc.LeafServerInterface;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.GlobalQueryPlan;
import nta.engine.planner.global.QueryStep;
import nta.engine.planner.global.UnitQuery;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.rpc.NettyRpc;
import nta.storage.StorageManager;
import nta.storage.Tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * @author jihoon
 *
 */
public class GlobalEngine implements EngineService {
	private Log LOG = LogFactory.getLog(GlobalEngine.class);
	
	private final Configuration conf;
	private final CatalogService catalog;
	private final QueryAnalyzer analyzer;
	private final StorageManager storageManager;
	private QueryContext.Factory factory;
	
	GlobalQueryPlanner globalPlanner;
	PhysicalPlanner phyPlanner;
	// RPC interface list for leaf servers
	LeafServerInterface leaf;
	
	public GlobalEngine(Configuration conf, CatalogService cat, StorageManager sm) throws IOException {
		this.conf = conf;
		this.catalog = cat;
		this.storageManager = sm;
		this.analyzer = new QueryAnalyzer(cat);
		this.factory = new QueryContext.Factory(catalog);
		
//		loPlanner = new LogicalPlanner(this.catalog);
		globalPlanner = new GlobalQueryPlanner(this.catalog);
		phyPlanner = new PhysicalPlanner(this.catalog, this.storageManager);
	}
	
	public void createTable(TableDesc meta) throws IOException {
		catalog.addTable(meta);
	}
	
	public String executeQuery(String querystr) throws Exception {
		String[] tokens = querystr.split(" ");
		String subqueryStr = null;
		String formatStr = "";
		for (int i = 0; i < tokens.length; i++) {
			if (tokens[i].equals("from")) {
				formatStr += "from %s ";
				i++;
			} else {
				formatStr += tokens[i] + " ";
			}
		}
		
	  QueryContext ctx = factory.create();
	  QueryBlock block = analyzer.parse(ctx, querystr);	    
	    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
	    LogicalOptimizer.optimize(ctx, plan);
	    GlobalQueryPlan globalPlan = globalPlanner.build(plan);
	    String storeName = null;
	    TableMeta meta = null;
	    for (int i = 0; i < globalPlan.size(); i++) {
	    	QueryStep queryStep = globalPlan.getQueryStep(i);
	    	for (int j = 0; j < queryStep.size(); j++) {
	    		UnitQuery q = queryStep.getQuery(j);
	    		if (storeName != null && q.getOp().getType() == ExprType.STORE) {
	    			storeName = ((StoreTableNode)q.getOp()).getTableName();
	    		} else if (meta != null && q.getOp().getType() == ExprType.GROUP_BY) {
	    			meta = new TableMetaImpl(((GroupbyNode)q.getOp()).getOutputSchema(), StoreType.CSV);
	    		}
	    		if (storeName != null && meta != null) {
	    			StorageManager sm = new StorageManager(conf);
	    			sm.initTableBase(meta, storeName);
	    			break;
	    		}
	    	}
	    }

		if (storeName == null) {
			storeName = "/" + System.currentTimeMillis();
		}

	    List<HostInfo> tabletServInfoList = null;
	    		
	    // execute sub queries via RPC
	    for (int i = 0; i < globalPlan.size(); i++) {
	    	QueryStep queryStep = globalPlan.getQueryStep(i);
	    	for (int j = 0 ; j < queryStep.size(); j++) {
	    		UnitQuery q = queryStep.getQuery(j);
	    		if (q.getTableName() == null) {
	    			// fill table name
	    			// fill fragments
	    		}
	    		if (q.getTableName() != null) {
	    		tabletServInfoList = catalog.getHostByTable(q.getTableName());
	    		
	    		for (HostInfo servInfo : tabletServInfoList) {
	    			if (servInfo.getFragment().equals(q.getFragments().get(0))) {
	    				q.setHost(servInfo.getHostName(), servInfo.getPort());
	    				break;
	    			}
	    		}
    			// fill host and port
	    		LOG.info("Host: " + q.getHost() + " port: " + q.getPort());
	    		
	    		if (q.getOp().getType() == ExprType.SCAN) {
		    		// make SubQueryRequest
	    			q.setOutputName(storeName);
	    			subqueryStr = String.format(formatStr, q.getFragments().get(0).getId());
	    			SubQueryRequest request = new SubQueryRequestImpl(q.getId(), q.getFragments(), 
	    					new URI(q.getOutputName()), subqueryStr);
			    	leaf = (LeafServerInterface) NettyRpc.getProtoParamBlockingRpcProxy(LeafServerInterface.class, 
			    			new InetSocketAddress(q.getHost(), q.getPort()));
			    	leaf.requestSubQuery(request.getProto());
	    		} else {
	    			// make ExecutorRunRequest
	    		}
	    		}
	    	}
	    	// wait for finishing the query step 
	    }
		
		return storeName;
	}
	
	public void execute(PhysicalOp op, ResultSetMemImplOld result) throws IOException {
		Tuple next = null;	

		while((next = op.next()) != null) {
			
			result.addTuple(next);
		}
	}
	
	private String tupleToString(Tuple t) {
		boolean first = true;
		StringBuilder sb = new StringBuilder();
		for(int i=0; i < t.size(); i++) {			
			if(t.get(i) != null) {
				if(first) {
					first = false;
				} else {
					sb.append("\t");
				}
				sb.append(t.get(i));
			}			
		}
		return sb.toString();
	}
	
	public void updateQuery(String nql) throws NTAQueryException  {
		
	}

	/* (non-Javadoc)
	 * @see nta.engine.EngineService#init()
	 */
	@Override
	public void init() throws IOException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see nta.engine.EngineService#shutdown()
	 */
	@Override
	public void shutdown() throws IOException {
		LOG.info(GlobalEngine.class.getName()+" is being stopped");
	}


	private String generateQuery(LogicalNode plan, Fragment fragment, String groupby) {
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
				from = fragment.getId();
				if (scan.hasTargetList()) {
					proj = "";
					for (Column te : scan.getTargetList().getColumns()) {
						if (proj.equals("")) {
							proj += te.getColumnName();
						} else {
							proj += ", " + te.getColumnName();
						}
					}
				}
				if (scan.hasQual()) {
					where = exprToString(scan.getQual());
				}
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
					proj += te.getColumnSchema().getColumnName() + ", ";
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
		if (groupby != null) {
			strQuery += groupby;
		}
		return strQuery;
	}
	
	private String exprToString(EvalNode expr) {
		String str = "";
		ArrayList<EvalNode> s = new ArrayList<EvalNode>();
		EvalNode e;
		s.add(expr);
		s.add(expr);
		
		while (!s.isEmpty()) {
			e = s.remove(0);
			if (s.size() > 0 && e.equals(s.get(0))) {
				// in
				if (e.getRightExpr() != null) {
					s.add(1, e.getRightExpr());
					s.add(1, e.getRightExpr());
				}
				if (e.getLeftExpr() != null) {
					s.add(0, e.getLeftExpr());	// in
					s.add(0, e.getLeftExpr());	// out
				}
			} else {
				// out
				str += getStringOfExpr(e) + " ";
			}
		}
		
		return str;
	}
	
	private String getStringOfExpr(EvalNode expr) {
		String ret = null;
		switch (expr.getType()) {
		case FIELD:
			FieldEval field = (FieldEval) expr;
			ret = field.getColumnName();
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
