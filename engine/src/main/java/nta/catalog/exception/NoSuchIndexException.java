/**
 * 
 */
package nta.catalog.exception;

/**
 * @author Hyunsik Choi
 */
public class NoSuchIndexException extends CatalogException {
  private static final long serialVersionUID = 3705839985189534673L;

  /**
   * 
   */
  public NoSuchIndexException() {
  }

  /**
   * @param message
   */
  public NoSuchIndexException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public NoSuchIndexException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public NoSuchIndexException(String message, Throwable cause) {
    super(message, cause);
  }
}
