package nta.catalog.exception;

/**
 * @author Hyunsik Choi
 */
public class AlreadyExistsFieldException extends RuntimeException {
	private static final long serialVersionUID = 6766228091940775275L;

	public AlreadyExistsFieldException() {
	}

	public AlreadyExistsFieldException(String fieldName) {
		super("Already Exists Field: "+fieldName);
	}
}
