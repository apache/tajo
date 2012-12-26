/**
 * 
 */
package tajo.engine.query.exception;


/**
 * @author hyunsik
 *
 */
public class NotSupportQueryException extends InvalidQueryException {
	private static final long serialVersionUID = 4079784008765680410L;

	/**
	 * @param query
	 */
	public NotSupportQueryException(String query) {
		super("Unsupported query: "+query);
	}
}
