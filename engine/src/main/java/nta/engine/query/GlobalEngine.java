/**
 * 
 */
package nta.engine.query;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.StringTokenizer;

import nta.catalog.Catalog;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableDescImpl;
import nta.engine.EngineService;
import nta.engine.ResultSetMemImplOld;
import nta.engine.ResultSetOld;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.PhysicalOp;
import nta.engine.ipc.LeafServerInterface;
import nta.engine.parser.NQL;
import nta.engine.parser.NQL.Query;
import nta.engine.plan.global.DecomposedQuery;
import nta.engine.plan.global.GenericTaskTree;
import nta.engine.plan.logical.LogicalPlan;
import nta.engine.plan.logical.OpType;
import nta.engine.plan.logical.RootOp;
import nta.storage.StorageManager;
import nta.storage.Tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * @author jihoon
 *
 */
public class GlobalEngine implements EngineService {
	private Log LOG = LogFactory.getLog(GlobalEngine.class);
	
	private final Configuration conf;
	private final Catalog catalog;
	private final StorageManager storageManager;
	private final NQL parser;
	
	LogicalPlanner loPlanner;
	GlobalQueryPlanner globalPlanner;
	PhysicalPlanner phyPlanner;
	// RPC interface list for leaf servers
	
	public GlobalEngine(Configuration conf, Catalog cat, StorageManager sm) throws IOException {
		this.conf = conf;
		this.catalog = cat;
		this.storageManager = sm;
		parser = new NQL(this.catalog);
		
		loPlanner = new LogicalPlanner(this.catalog);
		globalPlanner = new GlobalQueryPlanner(this.catalog);
		phyPlanner = new PhysicalPlanner(this.catalog, this.storageManager);
	}
	
	public void createTable(TableDescImpl meta) throws IOException {
		catalog.addTable(meta);
	}
	
	public ResultSetOld executeQuery(String querystr) throws Exception {
		Query query = parser.parse(querystr);
		LogicalPlan rawPlan = loPlanner.compile(query);
		if (rawPlan.getRoot().getType() == OpType.SHOW_TABLE || 
				rawPlan.getRoot().getType() == OpType.DESC_TABLE) {
			PhysicalOp plan = phyPlanner.compile(rawPlan, null);
			Schema meta = plan.getSchema();
			ResultSetMemImplOld rs = new ResultSetMemImplOld(meta);
			try {
				execute(plan, rs);
				return rs;
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		} else {
			RootOp root = new RootOp();
			root.setSubOp(rawPlan.getRoot());
			GenericTaskTree taskTree = globalPlanner.localize(new LogicalPlan(root));
			GenericTaskTree optimized = globalPlanner.optimize(taskTree);
			List<DecomposedQuery> queries = globalPlanner.decompose(optimized);
			// execute sub queries via RPC
			StringTokenizer tokenizer;
			for (DecomposedQuery q : queries) {
				tokenizer = new StringTokenizer(q.getHostName(), ":");
				LeafServerInterface leaf = (LeafServerInterface) RPC.getProxy(LeafServerInterface.class, 
						LeafServerInterface.versionID, new InetSocketAddress(tokenizer.nextToken(), 
								Integer.valueOf(tokenizer.nextToken())), conf);
				leaf.requestSubQuery(q.getQuery());
			}
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
