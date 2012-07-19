package nta.engine.exception;

/**
 * @author jihoon
 */
public class EmptyClusterException extends Exception {

  public EmptyClusterException() {

  }

  public EmptyClusterException(String msg) {
    super(msg);
  }

  public EmptyClusterException(Exception e) {
    super(e);
  }
}
