package nta.engine.query;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import nta.catalog.Catalog;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.conf.NtaConf;
import nta.engine.EngineService;
import nta.engine.ResultSet;
import nta.engine.ResultSetMemImpl;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.PhysicalOp;
import nta.engine.parser.NQL;
import nta.engine.parser.NQL.Query;
import nta.engine.plan.logical.LogicalPlan;
import nta.storage.StorageManager;
import nta.storage.VTuple;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class LocalEngine implements EngineService {
	private Log LOG = LogFactory.getLog(LocalEngine.class);
	
	private final NtaConf conf;
	private final Catalog catalog;
	private final StorageManager storageManager;
	private final NQL parser;
	
	LogicalPlanner loPlanner;
	LogicalPlanOptimizer loOptimizer;
	PhysicalPlanner phyPlanner;
	
	PrintStream stream;
	
	public LocalEngine(NtaConf conf, Catalog cat, StorageManager sm, PrintStream stream) {
		this.conf = conf;
		this.catalog = cat;
		this.storageManager = sm;
		parser = new NQL(this.catalog);
		
		loPlanner = new LogicalPlanner(this.catalog);
		loOptimizer = new LogicalPlanOptimizer();
		phyPlanner = new PhysicalPlanner(this.catalog, this.storageManager);
		
		this.stream = stream;
	}
	
	public void createTable(TableMeta meta) throws IOException {
		catalog.addTable(meta);
	}
	
	public ResultSet executeQuery(String querystr) throws NTAQueryException {
		Query query = parser.parse(querystr);
		LogicalPlan rawPlan = loPlanner.compile(query);
		LogicalPlan optimized = loOptimizer.optimize(rawPlan);
		PhysicalOp plan = phyPlanner.compile(optimized);
		
		Schema meta = plan.getSchema();
		ResultSetMemImpl rs = null;
		if(meta != null) {
			rs = new ResultSetMemImpl(meta);
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
	
	public void execute(PhysicalOp op, ResultSetMemImpl result) throws IOException {
		VTuple next = null;	
		
		if(stream != null) {
			for(Column desc : result.getSchema().getColumns()) {
				stream.print(desc.getName()+"\t");
			}
			stream.println("\n----------------------------------");
		}
		
		while((next = op.next()) != null) {
			if(stream != null) {
				stream.println(tupleToString(next));
			}
			
			result.addTuple(next);
		}
	}
	
	private String tupleToString(VTuple t) {
		boolean first = true;
		StringBuilder sb = new StringBuilder();
		for(int i=0; i < t.values.length; i++) {			
			if(t.values[i] != null) {
				if(first) {
					first = false;
				} else {
					sb.append("\t");
				}
				sb.append(t.values[i]);
			}			
		}
		return sb.toString();
	}

	public void updateQuery(String nql) throws NTAQueryException  {
		Query query = parser.parse(nql);
		LogicalPlan rawPlan = loPlanner.compile(query);
		LogicalPlan optimized = loOptimizer.optimize(rawPlan);
		PhysicalOp plan = phyPlanner.compile(optimized);
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
	public void shutdown() throws IOException {
		LOG.info(LocalEngine.class.getName()+" is being stopped");		
	}
}
