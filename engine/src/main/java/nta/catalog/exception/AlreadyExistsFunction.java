package nta.catalog.exception;

/**
 * @author Hyunsik Choi
 */
public class AlreadyExistsFunction extends CatalogException {
	private static final long serialVersionUID = 3224521585413794703L;

	public AlreadyExistsFunction() {}

	public AlreadyExistsFunction(String funcName) {
		super("Already Exists Function: "+funcName);
	}
}
