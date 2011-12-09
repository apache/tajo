package nta.catalog.exception;

import nta.engine.exception.NTAQueryException;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class AlreadyExistsTableException extends NTAQueryException {
	private static final long serialVersionUID = -641623770742392865L;

	public AlreadyExistsTableException() {		
	}

	public AlreadyExistsTableException(String tableName) {
		super("Already Exists Table: "+tableName);
	}
}
