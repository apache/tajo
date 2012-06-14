package nta.catalog.exception;

/**
 * @author Hyunsik Choi
 */
public class NoSuchFunctionException extends RuntimeException {
	private static final long serialVersionUID = 5062193018697228028L;

	public NoSuchFunctionException() {}

	public NoSuchFunctionException(String funcName) {
		super("No Such GeneralFunction in Catalog: "+funcName);
	}
}
