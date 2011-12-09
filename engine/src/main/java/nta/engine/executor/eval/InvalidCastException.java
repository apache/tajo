/**
 * 
 */
package nta.engine.executor.eval;

/**
 * @author Hyunsik Choi
 *
 */
public class InvalidCastException extends RuntimeException {
	private static final long serialVersionUID = -5090530469575858320L;

	public InvalidCastException() {
	}

	/**
	 * @param message
	 */
	public InvalidCastException(String message) {
		super(message);
	}
}
