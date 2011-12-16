package nta.engine;

public interface Abortable {
	/**
	   * Abort the server or client.
	   * @param why Why we're aborting.
	   * @param e Throwable that caused abort. Can be null.
	   */
	  public void abort(String why, Throwable e);
}
