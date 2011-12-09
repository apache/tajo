package nta.engine.exception;


/**
 * @author Hyunsik Choi
 *
 */
public class AmbiguousFieldException extends InvalidQueryException {
	private static final long serialVersionUID = 3102675985226352347L;

	/**
	 * 
	 */
	public AmbiguousFieldException() {
	}

	/**
	 * @param fieldName
	 */
	public AmbiguousFieldException(String fieldName) {
		super("Ambiguous Field Error: "+fieldName);	
	}
}
