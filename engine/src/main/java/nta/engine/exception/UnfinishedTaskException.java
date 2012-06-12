/**
 * 
 */
package nta.engine.exception;

/**
 * @author jihoon
 *
 */
public class UnfinishedTaskException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -3229141373378209229L;
  
  public UnfinishedTaskException() {
    
  }

  public UnfinishedTaskException(String message) {
    super(message);
  }
}
