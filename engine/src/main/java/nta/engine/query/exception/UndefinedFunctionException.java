/**
 * 
 */
package nta.engine.query.exception;


/**
 * @author Hyunsik Choi
 *
 */
public class UndefinedFunctionException extends InvalidQueryException {
	private static final long serialVersionUID = 113593927391549716L;

	/**
	 * @param signature
	 */
	public UndefinedFunctionException(String signature) {
		super("Error: call to undefined function "+signature+"()");	
	}
}
