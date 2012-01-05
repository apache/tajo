package nta.engine.query;

import java.util.ArrayList;
import java.util.List;

import nta.catalog.Schema;
import nta.catalog.exception.NoSuchTableException;
import nta.catalog.proto.TableProtos.StoreType;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.NotSupportQueryException;
import nta.engine.parser.NQL.Query;
import nta.engine.parser.RelInfo;
import nta.engine.plan.JoinType;
import nta.engine.plan.logical.ControlLO;
import nta.engine.plan.logical.CreateTableLO;
import nta.engine.plan.logical.DescTableLO;
import nta.engine.plan.logical.GroupByOp;
import nta.engine.plan.logical.InsertIntoLO;
import nta.engine.plan.logical.JoinOp;
import nta.engine.plan.logical.LogicalOp;
import nta.engine.plan.logical.LogicalPlan;
import nta.engine.plan.logical.OpType;
import nta.engine.plan.logical.ProjectLO;
import nta.engine.plan.logical.ScanOp;
import nta.engine.plan.logical.SelectionOp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import nta.catalog.Catalog;

public class LogicalPlanner {
	private Log LOG = LogFactory.getLog(LogicalPlanner.class);	
	
	private final Catalog catalog;
	
	public LogicalPlanner(Catalog cat) {
		this.catalog = cat;
	}
	
	public LogicalPlan compile(Query query) throws NTAQueryException {
		return new LogicalPlan(build(query));
	}
	
	public LogicalOp build(Query query) throws NTAQueryException {
		LogicalOp op = null;		

		switch(query.getCmdType()) {		
		
		case CREATE_TABLE:
			CreateTableLO lo = new CreateTableLO(query.getTargetTable(), 
				query.getTableDef());
			
			if(query.hasStoreType()) {
				if(query.getStoreName().equalsIgnoreCase("mem")) {
					lo.setStoreType(StoreType.MEM);
				} else if(query.getStoreName().equalsIgnoreCase("raw")) {
					lo.setStoreType(StoreType.RAW);
				} else {
					throw new NTAQueryException("Not supported Store Type: "+query.getStoreName());
				}
			} else {
				lo.setStoreType(StoreType.MEM);
			}
			
			if(query.hasSubQuery()) {
				lo.setSubQuery(build(query.getSubQuery()));
			}
			op = lo;
			
			break;
		case INSERT:
			op = buildInsertInto(query);
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
	
	public LogicalOp buildInsertInto(Query query) throws NoSuchTableException {
		int targetTableId = this.catalog.getTableId(query.getTargetTable());
		return new InsertIntoLO(targetTableId, query.getTargetList(), query.getValues());
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
			meta = this.catalog.getTableInfo(query.getTargetTable()).getSchema();
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