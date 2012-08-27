package tajo.engine.exception;

/**
 * @author jihoon
 */
public class IllegalQueryStatusException extends Exception {

  public IllegalQueryStatusException() {

  }

  public IllegalQueryStatusException(String msg) {
    super(msg);
  }

  public IllegalQueryStatusException(Exception e) {
    super(e);
  }

  public IllegalQueryStatusException(String msg, Exception e) {
    super(msg, e);
  }
}
