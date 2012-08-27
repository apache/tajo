package tajo.engine.query.exception;


public class NQLSyntaxException extends InvalidQueryException {
	private static final long serialVersionUID = 5388279335175632066L;

	public NQLSyntaxException(String errorQuery) {
		super("Syntax error: " + errorQuery);
	}
}
