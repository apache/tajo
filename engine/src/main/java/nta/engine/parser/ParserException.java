/**
 * 
 */
package nta.engine.parser;

/**
 * @author Hyunsik Choi
 */
public class ParserException extends RuntimeException {
  private static final long serialVersionUID = -4392842405539056555L;

  /**
   * 
   */
  public ParserException() {
  }

  /**
   * @param message
   */
  public ParserException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public ParserException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public ParserException(String message, Throwable cause) {
    super(message, cause);
  }
}
