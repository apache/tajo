/**
 * 
 */
package nta.engine.query;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import nta.catalog.CatalogServer;
import nta.catalog.Schema;
import nta.catalog.TableDescImpl;
import nta.engine.EngineService;
import nta.engine.QueryContext;
import nta.engine.ResultSetMemImplOld;
import nta.engine.ResultSetOld;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.PhysicalOp;
import nta.engine.ipc.LeafServerInterface;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.plan.global.DecomposedQuery;
import nta.engine.plan.global.GenericTaskGraph;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
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
	private final CatalogServer catalog;
	private final StorageManager storageManager;
	private QueryContext.Factory factory;
	
	GlobalQueryPlanner globalPlanner;
	PhysicalPlanner phyPlanner;
	// RPC interface list for leaf servers
	LeafServerInterface leaf;
	
	public GlobalEngine(Configuration conf, CatalogServer cat, StorageManager sm) throws IOException {
		this.conf = conf;
		this.catalog = cat;
		this.storageManager = sm;
		factory = new QueryContext.Factory(catalog);
		
//		loPlanner = new LogicalPlanner(this.catalog);
		globalPlanner = new GlobalQueryPlanner(this.catalog);
		phyPlanner = new PhysicalPlanner(this.catalog, this.storageManager);
	}
	
	public void createTable(TableDescImpl meta) throws IOException {
		catalog.addTable(meta);
	}
	
	public ResultSetOld executeQuery(String querystr) throws Exception {
		QueryBlock block = QueryAnalyzer.parse(querystr, catalog);
	    QueryContext ctx = factory.create(block);
	    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);

	    GenericTaskGraph taskTree = globalPlanner.localize(plan);
	    GenericTaskGraph optimized = globalPlanner.optimize(taskTree);
	    List<DecomposedQuery> queries = globalPlanner.decompose(optimized);
	    // execute sub queries via RPC
	    for (DecomposedQuery q : queries) {
	    	LOG.error("Host: " + q.getHostName() + " port: " + q.getPort());
	    	leaf = (LeafServerInterface) NettyRpc.getProtoParamBlockingRpcProxy(LeafServerInterface.class, 
	    			new InetSocketAddress(q.getHostName(), q.getPort()));
	    	leaf.requestSubQuery(((SubQueryRequestImpl)q.getQuery()).getProto());
	    }
		
		return null;
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

}
