package nta.engine.plan.global;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import nta.catalog.Catalog;
import nta.engine.plan.logical.LogicalOp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GlobalQueryPlanner {
	private static Log LOG = LogFactory.getLog(GlobalQueryPlanner.class);
	
	private Catalog catalog;
	private List<LogicalOp> reducedQueries;
	private Iterator<LogicalOp> it;
	
	public GlobalQueryPlanner(Catalog catalog) {
		this.catalog = catalog;
		reducedQueries = new ArrayList<LogicalOp>();
	}
	
	public void build(LogicalOp genericQuery) {
		localize(genericQuery);
	}
	
	public void localize(LogicalOp query) {
		
	}
	
	public void reduction(LogicalOp query) {
		
	}
	
	public LogicalOp nextQuery() {
		if (it.hasNext()) {
			return it.next();
		} else {
			return null;
		}
	}
}
