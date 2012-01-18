package nta.rpc;

public class RemoteException extends RuntimeException {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public RemoteException() {
    super();
  }

  public RemoteException(String message) {
    super(message);
  }

}
