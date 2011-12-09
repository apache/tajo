package nta.engine.exception;

public class InvalidQueryException extends RuntimeException {
	private static final long serialVersionUID = -7085849718839416246L;

	public InvalidQueryException() {
	}

	public InvalidQueryException(String errorQuery) {
		super("Invalid Query: " + errorQuery);
	}
}
