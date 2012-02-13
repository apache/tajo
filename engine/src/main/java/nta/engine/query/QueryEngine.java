package nta.engine.query;

import java.io.IOException;
import java.io.PrintStream;

import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.engine.EngineService;
import nta.engine.ResultSetMemImplOld;
import nta.engine.ResultSetOld;
import nta.engine.exception.InternalException;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.PhysicalOp;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.NQL;
import nta.engine.parser.NQL.Query;
import nta.engine.plan.logical.LogicalPlan;
import nta.storage.StorageManager;
import nta.storage.Tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class QueryEngine implements EngineService {
	private Log LOG = LogFactory.getLog(QueryEngine.class);
	
	private final Configuration conf;
	private final CatalogService catalog;
	private final StorageManager storageManager;
	private final NQL parser;
	
	LogicalPlanner loPlanner;
	LogicalPlanOptimizer loOptimizer;
	PhysicalPlanner phyPlanner;
	
	PrintStream stream;
	
	public QueryEngine(Configuration conf, CatalogService cat, StorageManager sm, PrintStream stream) {
		this.conf = conf;
		this.catalog = cat;
		this.storageManager = sm;
		parser = new NQL(this.catalog);
		
		loPlanner = new LogicalPlanner(this.catalog);
		loOptimizer = new LogicalPlanOptimizer();
		phyPlanner = new PhysicalPlanner(this.catalog, this.storageManager);
		
		this.stream = stream;
	}
	
	public void createTable(TableDesc meta) throws IOException {
		catalog.addTable(meta);
	}
	
	public ResultSetOld executeQuery(String querystr, Fragment tablet) throws NTAQueryException {
		Query query = parser.parse(querystr);
		LogicalPlan rawPlan = loPlanner.compile(query);
		LogicalPlan optimized = loOptimizer.optimize(rawPlan);
		PhysicalOp plan = null;
    try {
      plan = phyPlanner.compile(optimized, tablet);
    } catch (InternalException e1) {
      e1.printStackTrace();
    }
		
		Schema meta = plan.getSchema();
		ResultSetMemImplOld rs = null;
		if(meta != null) {
			rs = new ResultSetMemImplOld(meta);
			try {
				execute(plan, rs);
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		} else {
			try {
				plan.next();
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		}
		
		return rs;
	}

	public void execute(PhysicalOp op, ResultSetMemImplOld result) throws IOException {
		Tuple next = null;	
		
		if(stream != null) {
			for(Column desc : result.getSchema().getColumns()) {
				stream.print(desc.getName()+"\t");
			}
			stream.println("\n----------------------------------");
		}
		
		while((next = op.next()) != null) {
			System.out.println(next);
		  if(stream != null) {
				stream.println(tupleToString(next));
			}
			
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
		Query query = parser.parse(nql);
		LogicalPlan rawPlan = loPlanner.compile(query);
		LogicalPlan optimized = loOptimizer.optimize(rawPlan);
		PhysicalOp plan = null;
    try {
      plan = phyPlanner.compile(optimized, null);
    } catch (InternalException e) {
      e.printStackTrace();
    }
		try {
			plan.next();
		} catch (IOException ioe) {
			LOG.error(ioe.getMessage());
		}
	}

	@Override
	public void init() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdown() {
		LOG.info(QueryEngine.class.getName()+" is being stopped");		
	}
}
