package nta.engine.exception;

public class NQLSyntaxException extends NTAQueryException {
	private static final long serialVersionUID = 5388279335175632066L;

	public NQLSyntaxException() {
	}

	public NQLSyntaxException(String errorQuery) {
		super("Syntax Error: " + errorQuery);
	}
}
