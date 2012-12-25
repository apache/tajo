/**
 * 
 */
package tajo.engine.query.exception;

/**
 * @author Hyunsik Choi
 *
 */
public class InvalidEvalException extends InvalidQueryException {
	private static final long serialVersionUID = -2897003028483298256L;

	/**
	 * @param message
	 */
	public InvalidEvalException(String message) {
		super(message);
	}
}
