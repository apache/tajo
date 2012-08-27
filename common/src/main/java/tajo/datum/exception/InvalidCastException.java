/**
 * 
 */
package tajo.datum.exception;

/**
 * @author Hyunsik Choi
 *
 */
public class InvalidCastException extends RuntimeException {
	private static final long serialVersionUID = -7689027447969916148L;

	/**
	 * 
	 */
	public InvalidCastException() {
	}

	/**
	 * @param message
	 */
	public InvalidCastException(String message) {
		super(message);
	}
}
