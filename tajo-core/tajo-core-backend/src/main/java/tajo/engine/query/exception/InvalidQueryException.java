package tajo.engine.query.exception;

public class InvalidQueryException extends RuntimeException {
	private static final long serialVersionUID = -7085849718839416246L;

  public InvalidQueryException() {
    super();
  }

	public InvalidQueryException(String message) {
    super(message);
  }
	
	public InvalidQueryException(Throwable t) {
		super(t);
	}
}
