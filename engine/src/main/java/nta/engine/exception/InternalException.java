/**
 * 
 */
package nta.engine.exception;

/**
 * @author hyunsik
 *
 */
public class InternalException extends NTAQueryException {

	private static final long serialVersionUID = -262149616685882358L;

	/**
	 * 
	 */
	public InternalException() {
	}

	/**
	 * @param message
	 */
	public InternalException(String message) {
		super(message);
	}
}
