/**
 * 
 */
package nta.engine.query;

import java.io.IOException;
import java.net.URI;

import nta.catalog.Catalog;
import nta.catalog.TableInfo;
import nta.engine.exception.InternalProblemException;
import nta.engine.exec.CreateTableOp;
import nta.engine.exec.DescTableOp;
import nta.engine.exec.InsertIntoOp;
import nta.engine.exec.PhysicalOp;
import nta.engine.exec.PrintResult;
import nta.engine.exec.ProjectOp;
import nta.engine.exec.SelOp;
import nta.engine.exec.SeqScanOp;
import nta.engine.exec.ShowFunctionOp;
import nta.engine.exec.ShowTableOp;
import nta.engine.plan.logical.ControlLO;
import nta.engine.plan.logical.CreateTableLO;
import nta.engine.plan.logical.DescTableLO;
import nta.engine.plan.logical.InsertIntoLO;
import nta.engine.plan.logical.JoinOp;
import nta.engine.plan.logical.LogicalOp;
import nta.engine.plan.logical.LogicalPlan;
import nta.engine.plan.logical.ProjectLO;
import nta.engine.plan.logical.ScanOp;
import nta.engine.plan.logical.SelectionOp;
import nta.storage.Scanner;
import nta.storage.Store;
import nta.storage.StoreManager;

/**
 * @author Hyunsik Choi
 *
 */
public class PhysicalPlanner {
	Catalog cat;
	StoreManager sm;
	
	/**
	 * 
	 */
	public PhysicalPlanner(Catalog cat, StoreManager sm) {
		this.cat = cat;
		this.sm = sm;
	}
	
	public PhysicalOp compile(LogicalPlan plan) throws InternalProblemException {
		PhysicalOp op = null;
		try {
			op = buildPlan(plan.getRoot());			
		} catch (IOException ioe) {
			throw new InternalProblemException(ioe);
		}
		
		return op;
	}
	
	public PhysicalOp buildPlan(LogicalOp op) throws IOException {		
		PhysicalOp cur = null;
		
		PhysicalOp outer = null;
		PhysicalOp inner = null;
		
		switch(op.getType()) {
		case SCAN:
			cur = buildScanPlan(op);
			break;
		case SELECTION:;
			SelectionOp selOp = (SelectionOp) op;			
			cur = new SelOp(buildPlan(selOp.getSubOp()), selOp.getQual());
			break;
		case JOIN:
			JoinOp jOp = (JoinOp) op;
			outer = buildPlan(jOp.getOuter());
			inner = buildPlan(jOp.getInner());		
			break;
		case PROJECTION:;
			ProjectLO projectLO= (ProjectLO) op;
			cur = new ProjectOp(buildPlan(projectLO.getSubOp()), projectLO);
			break;
		case GROUP_BY:;
		case RENAME:;
		case SORT:;
		case SET_UNION:;
		case SET_DIFF:;
		case SET_INTERSECT:;
		
		case CREATE_TABLE:
			CreateTableLO createTableOp = (CreateTableLO) op;
			if(createTableOp.hasSubQuery()) {
				cur = buildPlan(createTableOp.getSubQuery());
			}
			cur = new CreateTableOp(createTableOp, cat, sm);
			break;
			
		case INSERT_INTO:
			InsertIntoLO insertLo = (InsertIntoLO) op;
			cur = new InsertIntoOp(cat, sm, insertLo);
			break;

		case SHOW_TABLE:
			ControlLO showTableOp = (ControlLO) op;
			cur = new ShowTableOp(showTableOp,this.cat);
			break;
			
		case DESC_TABLE:
			DescTableLO lo = (DescTableLO) op;
			cur = new DescTableOp(lo);
			break;
			
		case SHOW_FUNCTION:
			ControlLO showFuncOp = (ControlLO) op;
			cur = new ShowFunctionOp(showFuncOp,this.cat);
		}
		
		return cur;
	}
	
	public PhysicalOp buildScanPlan(LogicalOp op) throws IOException {
		ScanOp sOp = (ScanOp) op;
		
		TableInfo info = cat.getTableInfo(sOp.getRelName()); 
		Scanner scanner = sm.getScanner(info.getStore());		
		
		return new SeqScanOp(scanner);
	}
}
