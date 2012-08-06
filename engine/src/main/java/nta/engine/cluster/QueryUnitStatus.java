package nta.engine.cluster;

import com.google.common.collect.Maps;
import nta.engine.MasterInterfaceProtos.*;
import nta.engine.QueryUnitId;

import java.util.Map;

/**
 * @author jihoon
 */
public class QueryUnitStatus {
  public static class QueryUnitAttempt {
    private int id;
    private volatile QueryStatus status;

    public QueryUnitAttempt(int id, QueryStatus status) {
      this.id = id;
      this.status = status;
    }

    public synchronized void setStatus(QueryStatus status) {
      this.status = status;
    }

    public int getId() {
      return id;
    }

    public QueryStatus getStatus() {
      return this.status;
    }
  }

  private QueryUnitId queryUnitId;
  private Map<Integer, QueryUnitAttempt> attemptMap;
  private volatile int lastAttemptId;

  public QueryUnitStatus(QueryUnitId queryUnitId) {
    this.queryUnitId = queryUnitId;
    this.attemptMap = Maps.newConcurrentMap();
    this.lastAttemptId = -1;
  }

  public synchronized void putAttempt(QueryUnitAttempt attempt) {
    if (lastAttemptId < attempt.getId()) {
      lastAttemptId = attempt.getId();
    }
    attemptMap.put(attempt.getId(), attempt);
  }

  public QueryUnitId getQueryUnitId() {
    return this.queryUnitId;
  }

  public QueryUnitAttempt getAttempt(int id) {
    return this.attemptMap.get(id);
  }

  public Map<Integer, QueryUnitAttempt> getAttemptMap() {
    return this.attemptMap;
  }

  public QueryUnitAttempt getLastAttempt() {
    return this.attemptMap.get(lastAttemptId);
  }

  public int getLastAttemptId() {
    return this.lastAttemptId;
  }
}
