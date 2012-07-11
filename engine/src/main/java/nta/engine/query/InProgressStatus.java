/**
 * 
 */
package nta.engine.query;

import nta.engine.MasterInterfaceProtos.QueryStatus;

/**
 * @author jihoon
 *
 */
public class InProgressStatus {

  private float progress;
  private QueryStatus status;
  
  public InProgressStatus() {
    this.setProgress(0.f);
    this.setStatus(QueryStatus.QUERY_INITED);
  }
  
  public InProgressStatus(float progress, QueryStatus status) {
    this.setProgress(progress);
    this.setStatus(status);
  }
  
  public void setProgress(float progress) {
    this.progress = progress;
  }
  
  public void setStatus(QueryStatus status) {
    this.status = status;
  }
  
  public float getProgress() {
    return this.progress;
  }
  
  public QueryStatus getStatus() {
    return this.status;
  }
  
  @Override
  public String toString() {
    return "(PROGRESS: " + this.progress + " STATUS: " + this.status + ")";
  }
}
