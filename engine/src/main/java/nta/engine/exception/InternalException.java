/**
 * 
 */
package nta.engine.exception;

/**
 * @author hyunsik
 *
 */
public class InternalException extends Exception {

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

	/**
	 * @param cause
	 */
	public InternalException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public InternalException(String message, Throwable cause) {
		super(message, cause);
	}
}
