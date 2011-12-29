/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.executor.eval.Expr;
import nta.engine.query.TargetEntry;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 *
 */
public class SelOp extends PhysicalOp {
	private PhysicalOp inner = null;
	private Expr qual = null;
	private Tuple tuple = null;
	private Schema schema = null;
	private TargetEntry [] tlist = null;

	/**
	 * 
	 */
	public SelOp(PhysicalOp inner, Expr qual) {
		this.inner = inner;
		this.qual = qual;
		this.schema = inner.getSchema();
	}

	public Tuple buildTuple(Tuple tuple) {
		return tuple;
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

		while ((next = inner.next()) != null) {				
			tuple = buildTuple(next);
			if(qual.eval(tuple).asBool()) {				
				return tuple;
			}
		}
		return null;
	}	

	@Override
	public Schema getSchema() {
		// TODO - 만약 projection 값이 있다면 projected된 schema를 반환
		return this.schema;
	}
}
