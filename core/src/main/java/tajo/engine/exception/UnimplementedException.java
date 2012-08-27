package tajo.engine.exception;

/**
 * @author Hyunsik Choi
 */
public class UnimplementedException extends RuntimeException {
  private static final long serialVersionUID = -5467580471721530536L;

  public UnimplementedException() {
  }

  public UnimplementedException(String message) {
    super(message);
  }

  public UnimplementedException(Throwable cause) {
    super(cause);
  }

  public UnimplementedException(String message, Throwable cause) {
    super(message, cause);
  }
}
