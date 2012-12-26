/**
 * 
 */
package tajo.catalog.exception;

/**
 * @author Hyunsik Choi
 */
public class AlreadyExistsIndexException extends CatalogException {
  private static final long serialVersionUID = 3705839985189534673L;

  /**
   * 
   */
  public AlreadyExistsIndexException() {
  }

  /**
   * @param message
   */
  public AlreadyExistsIndexException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public AlreadyExistsIndexException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public AlreadyExistsIndexException(String message, Throwable cause) {
    super(message, cause);
  }
}
