/**
 * 
 */
package nta.engine;

import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.query.InProgressStatus;

/**
 * @author jihoon
 *
 */
public abstract class AbstractQuery {

  private InProgressStatus status;

  public AbstractQuery() {
    this.status = new InProgressStatus();
  }

  public synchronized void setInProgressStatus(InProgressStatus status) {
    this.status.setProgress(status.getProgress());
    this.status.setStatus(status.getStatus());
  }

  public void setProgress(float progress) {
    this.status.setProgress(progress);
  }
  
  public void setStatus(QueryStatus status) {
    this.status.setStatus(status);
  }
  
  public InProgressStatus getInProgressStatus() {
    return this.status;
  }
  
  public QueryStatus getStatus() {
    return this.status.getStatus();
  }
  
  public float getProgress() {
    return status.getProgress();
  }
}
