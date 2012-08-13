package nta.engine.planner.global;

import nta.catalog.statistics.TableStat;
import nta.engine.AbstractQuery;
import nta.engine.MasterInterfaceProtos.*;
import nta.engine.QueryUnitAttemptId;

/**
 * @author jihoon
 */
public class QueryUnitAttempt extends AbstractQuery {
  private final static int EXPIRE_TIME = 15000;

  private final QueryUnitAttemptId id;
  private final QueryUnit queryUnit;

  private String hostName;
  private int expire;
  private QueryStatus status;

  public QueryUnitAttempt(QueryUnitAttemptId id, QueryUnit queryUnit) {
    this.id = id;
    this.expire = QueryUnitAttempt.EXPIRE_TIME;
    this.queryUnit = queryUnit;
  }

  public QueryUnitAttemptId getId() {
    return this.id;
  }

  public QueryUnit getQueryUnit() {
    return this.queryUnit;
  }

  public QueryStatus getStatus() {
    return status;
  }

  public String getHost() {
    return this.hostName;
  }

  public void setStatus(QueryStatus status) {
    this.status = status;
  }

  public void setHost(String host) {
    this.hostName = host;
  }

  /*
    * Expire time
    */

  public synchronized void setExpireTime(int expire) {
    this.expire = expire;
  }

  public synchronized void updateExpireTime(int period) {
    this.setExpireTime(this.expire - period);
  }

  public synchronized void resetExpireTime() {
    this.setExpireTime(QueryUnitAttempt.EXPIRE_TIME);
  }

  public int getLeftTime() {
    return this.expire;
  }

  public void updateProgress(InProgressStatusProto progress) {
    if (status != progress.getStatus()) {
      this.setProgress(progress.getProgress());
      this.setStatus(progress.getStatus());
      if (progress.getPartitionsCount() > 0) {
        this.getQueryUnit().setPartitions(progress.getPartitionsList());
      }
      if (progress.hasResultStats()) {
        this.getQueryUnit().setStats(new TableStat(progress.getResultStats()));
      }
    }
    this.resetExpireTime();
  }
}
