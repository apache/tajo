/**
 * 
 */
package nta.catalog.exception;

import nta.engine.exception.InternalException;

/**
 * @author hyunsik
 *
 */
public class InvalidTableException extends InternalException {

	private static final long serialVersionUID = -6326266814969872171L;

	public InvalidTableException() {
	}

	/**
	 * @param message
	 */
	public InvalidTableException(String message) {
		super(message);
	}
}
