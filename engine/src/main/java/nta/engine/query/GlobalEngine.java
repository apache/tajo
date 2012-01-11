/**
 * 
 */
package nta.engine.query;

import java.io.IOException;
import java.util.List;

import nta.catalog.Catalog;
import nta.catalog.TableDescImpl;
import nta.engine.EngineService;
import nta.engine.ResultSetOld;
import nta.engine.exception.NTAQueryException;
import nta.engine.parser.NQL;
import nta.engine.parser.NQL.Query;
import nta.engine.plan.global.DecomposedQuery;
import nta.engine.plan.global.GenericTaskTree;
import nta.engine.plan.logical.LogicalPlan;
import nta.storage.StorageManager;

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
	private final Catalog catalog;
	private final StorageManager storageManager;
	private final NQL parser;
	
	LogicalPlanner loPlanner;
	GlobalQueryPlanner globalPlanner;
	// RPC interface list for leaf servers
	
	public GlobalEngine(Configuration conf, Catalog cat, StorageManager sm) throws IOException {
		this.conf = conf;
		this.catalog = cat;
		this.storageManager = sm;
		parser = new NQL(this.catalog);
		
		loPlanner = new LogicalPlanner(this.catalog);
		globalPlanner = new GlobalQueryPlanner(this.catalog);
	}
	
	public void createTable(TableDescImpl meta) throws IOException {
		catalog.addTable(meta);
	}
	
	public ResultSetOld executeQuery(String querystr) throws NTAQueryException {
		Query query = parser.parse(querystr);
		LogicalPlan rawPlan = loPlanner.compile(query);
		GenericTaskTree taskTree = globalPlanner.localize(rawPlan);
		GenericTaskTree optimized = globalPlanner.optimize(taskTree);
		List<DecomposedQuery> queries = globalPlanner.decompose(optimized);
		// execute sub queries via RPC
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
