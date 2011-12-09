/**
 * 
 */
package nta.engine.exception;

/**
 * @author Hyunsik Choi
 *
 */
public class UnknownFunctionException extends InvalidQueryException {
	private static final long serialVersionUID = 113593927391549716L;

	/**
	 * 
	 */
	public UnknownFunctionException() {
	}

	/**
	 * @param funcName
	 */
	public UnknownFunctionException(String funcName) {
		super("Unknown Function: "+funcName);	
	}
}
