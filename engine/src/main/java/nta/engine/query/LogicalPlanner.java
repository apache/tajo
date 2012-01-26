package nta.engine.query;

import java.util.ArrayList;
import java.util.List;

import nta.catalog.CatalogServer;
import nta.catalog.Schema;
import nta.catalog.exception.NoSuchTableException;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.NotSupportQueryException;
import nta.engine.parser.NQL.Query;
import nta.engine.parser.RelInfo;
import nta.engine.plan.JoinType;
import nta.engine.plan.logical.ControlLO;
import nta.engine.plan.logical.DescTableLO;
import nta.engine.plan.logical.GroupByOp;
import nta.engine.plan.logical.JoinOp;
import nta.engine.plan.logical.LogicalOp;
import nta.engine.plan.logical.LogicalPlan;
import nta.engine.plan.logical.OpType;
import nta.engine.plan.logical.ProjectLO;
import nta.engine.plan.logical.ScanOp;
import nta.engine.plan.logical.SelectionOp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LogicalPlanner {
	private Log LOG = LogFactory.getLog(LogicalPlanner.class);	
	
	private final CatalogServer catalog;
	
	public LogicalPlanner(CatalogServer cat) {
		this.catalog = cat;
	}
	
	public LogicalPlan compile(Query query) throws NTAQueryException {
		return new LogicalPlan(build(query));
	}
	
	public LogicalOp build(Query query) throws NTAQueryException {
		LogicalOp op = null;		

		switch(query.getCmdType()) {		
		
		case CREATE_TABLE:
			break;
		case INSERT:
			break;
		
		case SELECT:
			op = buildSelect(query);
			break;

		case SHOW_TABLES:
			op = new ControlLO(OpType.SHOW_TABLE);
			break;
			
		case SHOW_FUNCTION:
			op = new ControlLO(OpType.SHOW_FUNCTION);
			break;
			
		case DESC_TABLE:
			op = buildDescTable(query);
			break;
		default:;
			throw new NotSupportQueryException(query.toString());
		}
		
		return op;
	}
	
	/**
	 * ^(DESC_TABLE table)
	 * @param query
	 * @return
	 * @throws NoSuchTableException 
	 */
	public LogicalOp buildDescTable(Query query) throws NoSuchTableException {
		Schema meta;
		try {
			meta = this.catalog.getTableDesc(query.getTargetTable()).getMeta().getSchema();
		} catch (NoSuchTableException e) {
			throw new NoSuchTableException(query.getTargetTable());
		}
		return new DescTableLO(meta);
	}
	
	/**
	 * ^(SELECT from_clause? where_clause? groupby_clause? selectList)
	 * 
	 * @param ast
	 * @param state
	 * @return
	 */
	public LogicalOp buildSelect(Query query) {
		LogicalOp op = null; 
				
		if(query.hasFromClause()) {
			List<ScanOp> scanOps = new ArrayList<ScanOp>();
			for(RelInfo relInfo: query.getBaseRels()) {
				scanOps.add(new ScanOp(relInfo));
			}

			if(scanOps.size() == 2) {
				JoinOp jOp = new JoinOp(JoinType.INNER);
				jOp.setOuter(scanOps.get(0));
				jOp.setInner(scanOps.get(1));
				op = jOp;
			} else if(scanOps.size() == 1) {
				op = scanOps.get(0);		
			} else { // tableNum == 0
				
			}
		}
		
		if(query.hasWhereClause()) {
			SelectionOp sOp = new SelectionOp();
			sOp.setQual(query.getWhereCond());
			sOp.setSubOp(op);
			op = sOp;
		}
		
		if(query.hasGroupByClause()) {
			GroupByOp gOp = new GroupByOp();
			gOp.setSubOp(op);
			op = gOp;
		}
		
		if(query.asterisk()) {
			
		} else {
			TargetEntry [] target = query.getTargetList();			
			ProjectLO pOp = new ProjectLO(target);
			pOp.setSubOp(op);
			op = pOp;
		}		
		
		return op;	
	}
}