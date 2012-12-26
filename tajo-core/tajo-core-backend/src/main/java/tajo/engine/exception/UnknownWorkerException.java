/**
 * 
 */
package tajo.engine.exception;

/**
 * @author jihoon
 *
 */
public class UnknownWorkerException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -3677733092100608744L;
  private String unknownName;

  public UnknownWorkerException(String unknownName) {
    this.unknownName = unknownName;
  }

  public UnknownWorkerException(String unknownName, Exception e) {
    super(e);
    this.unknownName = unknownName;
  }

  public String getUnknownName() {
    return this.unknownName;
  }
}
