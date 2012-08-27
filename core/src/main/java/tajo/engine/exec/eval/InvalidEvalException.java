/**
 * 
 */
package tajo.engine.exec.eval;

/**
 * @author Hyunsik Choi
 *
 */
public class InvalidEvalException extends RuntimeException {
	private static final long serialVersionUID = -2897003028483298256L;

	public InvalidEvalException() {
	}

	/**
	 * @param message
	 */
	public InvalidEvalException(String message) {
		super(message);
	}
}
