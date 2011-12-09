/**
 * 
 */
package nta.engine.exec;

import java.io.IOException;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.executor.eval.Expr;
import nta.engine.query.TargetEntry;
import nta.storage.MemTuple;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 *
 */
public class SelOp extends PhysicalOp {
	private PhysicalOp inner = null;
	private Expr qual = null;
	private VTuple tuple = null;
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

	public VTuple buildTuple2(VTuple tuple) {
		return tuple;
	}

	public Tuple buildTuple(Tuple tuple) {
		MemTuple t = new MemTuple();
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
					t.putInt(resId, expr.eval(tuple).asInt());
					break;
				case LONG:
					t.putLong(resId, expr.eval(tuple).asLong());
				case FLOAT:
					t.putFloat(resId, expr.eval(tuple).asFloat());
				case DOUBLE:
					t.putDouble(resId, expr.eval(tuple).asDouble());
					break;
				case STRING:
					t.putString(resId, expr.eval(tuple).asChars());
					break;
				case BOOLEAN:
					t.putBoolean(resId, expr.eval(tuple).asBool());
				case ANY:
					t.putString(resId, expr.eval(tuple).asChars());
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
	public VTuple next() throws IOException {	
		VTuple next = null;					

		while ((next = inner.next()) != null) {				
			tuple = buildTuple2(next);
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
