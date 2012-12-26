package tajo.catalog.exception;

/**
 * @author Hyunsik Choi
 */
public class AlreadyExistsFunctionException extends CatalogException {
	private static final long serialVersionUID = 3224521585413794703L;

	public AlreadyExistsFunctionException(String funcName) {
		super("Already Exists Function: "+funcName);
	}
}
