/**
 * 
 */
package nta.engine.query;

import java.io.IOException;

import nta.catalog.Catalog;
import nta.catalog.TableDesc;
import nta.engine.exception.InternalException;
import nta.engine.exec.DescTableOp;
import nta.engine.exec.PhysicalOp;
import nta.engine.exec.ProjectOp;
import nta.engine.exec.SelOp;
import nta.engine.exec.SeqScanOp;
import nta.engine.exec.ShowFunctionOp;
import nta.engine.exec.ShowTableOp;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.plan.logical.ControlLO;
import nta.engine.plan.logical.DescTableLO;
import nta.engine.plan.logical.JoinOp;
import nta.engine.plan.logical.LogicalOp;
import nta.engine.plan.logical.LogicalPlan;
import nta.engine.plan.logical.ProjectLO;
import nta.engine.plan.logical.ScanOp;
import nta.engine.plan.logical.SelectionOp;
import nta.storage.Scanner;
import nta.storage.StorageManager;

/**
 * @author Hyunsik Choi
 *
 */
public class PhysicalPlanner {
	Catalog cat;
	StorageManager sm;
	
	/**
	 * 
	 */
	public PhysicalPlanner(Catalog cat, StorageManager sm) {
		this.cat = cat;
		this.sm = sm;
	}
	
	public PhysicalOp compile(LogicalPlan plan, Fragment tablet) throws InternalException {
		PhysicalOp op = null;
		try {
			op = buildPlan(plan.getRoot(), tablet);
		} catch (IOException ioe) {
			throw new InternalException(ioe.getMessage());
		}
		
		return op;
	}
	
	public PhysicalOp buildPlan(LogicalOp op, Fragment tablet) throws IOException {		
		PhysicalOp cur = null;
		
		PhysicalOp outer = null;
		PhysicalOp inner = null;
		
		switch(op.getType()) {
		case SCAN:
			cur = buildScanPlan(op, tablet);
			break;
		case SELECTION:;
			SelectionOp selOp = (SelectionOp) op;			
			cur = new SelOp(buildPlan(selOp.getSubOp(), tablet), selOp.getQual());
			break;
		case JOIN:
			JoinOp jOp = (JoinOp) op;
			outer = buildPlan(jOp.getOuter(), tablet);
			inner = buildPlan(jOp.getInner(), tablet);		
			break;
		case PROJECTION:;
			ProjectLO projectLO= (ProjectLO) op;
			cur = new ProjectOp(buildPlan(projectLO.getSubOp(), tablet), projectLO);
			break;
		case GROUP_BY:;
		case RENAME:;
		case SORT:;
		case SET_UNION:;
		case SET_DIFF:;
		case SET_INTERSECT:;
		
		case CREATE_TABLE:
			break;
			
		case INSERT_INTO:
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
	
	public PhysicalOp buildScanPlan(LogicalOp op, Fragment tablet) throws IOException {
		ScanOp sOp = (ScanOp) op;
		
		TableDesc info = cat.getTableDesc(sOp.getRelName()); 
		Scanner scanner = sm.getScanner(info.getMeta(), new Fragment [] {tablet});
		
		return new SeqScanOp(scanner);
	}
}
