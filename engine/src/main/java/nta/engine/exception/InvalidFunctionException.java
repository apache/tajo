/**
 * 
 */
package nta.engine.exception;

/**
 * @author Hyunsik Choi
 *
 */
public class InvalidFunctionException extends RuntimeException {

	private static final long serialVersionUID = -7521077594074673741L;

	public InvalidFunctionException() {
	}

	/**
	 * @param message
	 */
	public InvalidFunctionException(String message) {
		super(message);	
	}
}
