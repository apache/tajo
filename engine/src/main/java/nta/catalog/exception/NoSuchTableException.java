package nta.catalog.exception;

import nta.engine.exception.NTAQueryException;

/**
 * @author Hyunsik Choi
 */
public class NoSuchTableException extends NTAQueryException {
	private static final long serialVersionUID = 277182608283894937L;

	public NoSuchTableException() {}

	public NoSuchTableException(String relName) {
		super("No Such Relation in Catalog: "+relName);
	}
}
