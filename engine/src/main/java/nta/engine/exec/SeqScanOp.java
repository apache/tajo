/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.executor.eval.Expr;
import nta.engine.query.TargetEntry;
import nta.storage.Scanner;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 *
 */
public class SeqScanOp extends PhysicalOp {
	Scanner scanner = null;
	Expr qual = null;	
	boolean hasRead = true;	
	boolean nextAvail = false;	
	Schema schema = null;
	TargetEntry [] tlist = null;

	/**
	 * 
	 */
	public SeqScanOp(Scanner scanner) {
		this.scanner = scanner;
		this.schema = scanner.getSchema();
	}

	public SeqScanOp(Scanner scanner, TargetEntry [] tlist) {
		this(scanner);
		this.tlist = tlist;
	}
	
	public VTuple buildTuple2(VTuple tuple) {
		int [] projectList = null;
		VTuple resTuple = new VTuple(projectList.length);
		
		for(int i=0; i < projectList.length; i++) {
			resTuple.put(projectList[i], tuple.get(projectList[i]));
		}
		
		return resTuple;
	}

	public Tuple buildTuple(Tuple tuple) {
		VTuple t = new VTuple(tuple.size());
		Column field = null;

		if(tlist != null) {
			Expr expr;
			int resId;
			for(int i=0; i < tlist.length; i++) {
				expr = tlist[i].expr;
				resId = tlist[i].resId;
				field = schema.getColumn(tlist[i].colId);
				switch(field.getDataType()) {
				case INT:
					t.put(resId, expr.eval(tuple).asInt());
					break;
				case LONG:
					t.put(resId, expr.eval(tuple).asLong());
				case FLOAT:
					t.put(resId, expr.eval(tuple).asFloat());
				case DOUBLE:
					t.put(resId, expr.eval(tuple).asDouble());
					break;
				case STRING:
					t.put(resId, expr.eval(tuple).asChars());
					break;
				case BOOLEAN:
					t.put(resId, expr.eval(tuple).asBool());
				case ANY:
					t.put(resId, expr.eval(tuple).asChars());
				}
			}
			return t;
		} else {
			return tuple;
		}
	}

	public void setQual(Expr qual) {
		this.qual = qual;
	}

	/* (non-Javadoc)
	 * @see nta.query.executor.ScanExec#hasNextTuple()
	 */
	@Override
	public Tuple next() throws IOException {	
		Tuple next = null;					
		if(qual == null) {
			if((next = scanner.next()) != null) {
				return buildTuple(next);
			} else {
				return null;
			}
		} else {				
			while ((next = scanner.next()) != null) {				
				next = buildTuple(next);
				if(qual.eval(next).asBool()) {				
					return next;
				}
			}
			return null;
		}	
	}

	@Override
	public Schema getSchema() {
		// TODO - 만약 projection 값이 있다면 projected된 schema를 반환
		return this.schema;
	}
}
