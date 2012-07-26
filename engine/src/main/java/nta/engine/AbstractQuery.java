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

  public synchronized void setProgress(float progress) {
    this.status.setProgress(progress);
  }
  
  public synchronized void setStatus(QueryStatus status) {
    this.status.setStatus(status);
  }
  
  public synchronized InProgressStatus getInProgressStatus() {
    return this.status;
  }
  
  public synchronized QueryStatus getStatus() {
    return this.status.getStatus();
  }
  
  public synchronized float getProgress() {
    return status.getProgress();
  }
}
