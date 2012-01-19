package nta.engine.query.exception;



/**
 * @author Hyunsik Choi
 *
 */
public class AmbiguousFieldException extends InvalidQueryException {
	private static final long serialVersionUID = 3102675985226352347L;

	/**
	 * @param fieldName
	 */
	public AmbiguousFieldException(String fieldName) {
		super("ERROR: column name "+ fieldName + " is ambiguous");	
	}
}
