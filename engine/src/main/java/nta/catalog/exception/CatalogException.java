/**
 * 
 */
package nta.catalog.exception;

import java.io.IOException;

/**
 * @author hyunsik
 *
 */
public class CatalogException extends RuntimeException {
  private static final long serialVersionUID = -26362412527118618L;

  /**
   * 
   */
  public CatalogException() {
  }

  /**
   * @param message
   */
  public CatalogException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public CatalogException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public CatalogException(String message, Throwable cause) {
    super(message, cause);
  }
}