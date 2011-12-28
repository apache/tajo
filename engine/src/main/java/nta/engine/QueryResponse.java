/**
 * 
 */
package nta.engine;


/**
 * @author jimin
 * 
 */
public interface QueryResponse {
	public ResultSetOld get();

	public void cancel(boolean mayInterruptIfRunnging);

	public boolean isCanceled();
	
	public boolean isDone();
	
	public float getProgress();
}
