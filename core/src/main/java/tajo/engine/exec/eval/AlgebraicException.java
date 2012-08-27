/**
 * 
 */
package tajo.engine.exec.eval;

public class AlgebraicException extends RuntimeException {
  private static final long serialVersionUID = -1813125460274622006L;
  
  public AlgebraicException() {
  }

  public AlgebraicException(String message) {
    super(message);
  }

  public AlgebraicException(Throwable cause) {
    super(cause);
  }

  public AlgebraicException(String message, Throwable cause) {
    super(message, cause);
  }
}
