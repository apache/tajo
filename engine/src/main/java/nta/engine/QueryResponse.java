/**
 * 
 */
package nta.engine;


/**
 * @author jimin
 * 
 */
public interface QueryResponse {
	public ResultSet get();

	public void cancel(boolean mayInterruptIfRunnging);

	public boolean isCanceled();
	
	public boolean isDone();
	
	public float getProgress();
}
