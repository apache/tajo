/**
 * 
 */
package nta.datum.exception;

/**
 * @author Hyunsik Choi
 *
 */
public class InvalidOperationException extends RuntimeException {
	private static final long serialVersionUID = -7689027447969916148L;

	/**
	 * 
	 */
	public InvalidOperationException() {
	}

	/**
	 * @param message
	 */
	public InvalidOperationException(String message) {
		super(message);
	}
}
