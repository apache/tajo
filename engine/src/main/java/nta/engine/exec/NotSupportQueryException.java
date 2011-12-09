/**
 * 
 */
package nta.engine.exec;

import nta.engine.exception.NTAQueryException;

/**
 * @author hyunsik
 *
 */
public class NotSupportQueryException extends NTAQueryException {
	private static final long serialVersionUID = 4079784008765680410L;

	/**
	 * 
	 */
	public NotSupportQueryException() {
		super();
	}

	/**
	 * @param query
	 */
	public NotSupportQueryException(String query) {
		super("Not Support Query: "+query);
	}

}
