/**
 * 
 */
package nta.engine.executor;

import java.io.IOException;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.executor.eval.Expr;
import nta.storage.Scanner;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 *
 */
public class SeqScanExec implements ScanExec {
	Scanner scanner = null;
	Expr qual = null;
	Tuple tuple = null;
	boolean hasRead = true;	
	boolean nextAvail = false;	
	Schema schema = null;
	Expr [] cols = null;

	/**
	 * 
	 */
	public SeqScanExec(Scanner scanner, Schema schema) {
		this.scanner = scanner;
		this.schema = schema;
	}

	public SeqScanExec(Scanner scanner, Schema schema, Expr [] proj) {
		this.scanner = scanner;
		this.schema = schema;
		this.cols = proj;
	}

	public Tuple buildTuple(Tuple tuple) {
		Tuple t = new VTuple(tuple.size());
		Column field = null;

		if(cols != null) {		
			for(int i=0; i < cols.length; i++) {
				field = schema.getColumn(i);
				switch(field.getDataType()) {
				case INT:
					t.put(i, cols[i].eval(tuple).asInt());
					break;
				case LONG:
					t.put(i, cols[i].eval(tuple).asLong());
				case FLOAT:
					t.put(i, cols[i].eval(tuple).asFloat());
				case DOUBLE:
					t.put(i, cols[i].eval(tuple).asDouble());
					break;
				case STRING:
					t.put(i, cols[i].eval(tuple).asChars());
					break;
				case BOOLEAN:
					t.put(i, cols[i].eval(tuple).asBool());
				case ANY:
					t.put(i, cols[i].eval(tuple).asChars());
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
	 * @see nta.query.executor.ScanExec#next()
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
				tuple = buildTuple(next);
				if(qual.eval(tuple).asBool()) {				
					return tuple;
				}
			}
			return null;
		}	
	}
}
