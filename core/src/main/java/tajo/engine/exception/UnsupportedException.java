package tajo.engine.exception;

/**
 * @author Hyunsik Choi
 */
public class UnsupportedException extends RuntimeException {
  private static final long serialVersionUID = 6702291354858193578L;

  public UnsupportedException() {
  }

  public UnsupportedException(String message) {
    super(message);
  }

  public UnsupportedException(Throwable cause) {
    super(cause);
  }

  public UnsupportedException(String message, Throwable cause) {
    super(message, cause);
  }
}
