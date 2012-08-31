/**
 * 
 */
package tajo;

import tajo.util.TajoIdUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jihoon
 *
 */
public class QueryIdFactory {
  private static AtomicInteger nextId = 
      new AtomicInteger(-1);
  
  public static void reset() {
    nextId.set(-1);
  }

  public synchronized static QueryId newQueryId() {
    return TajoIdUtils.createQueryId(System.currentTimeMillis(), nextId.incrementAndGet());
  }
  
  public synchronized static SubQueryId newSubQueryId(QueryId queryId) {
    return new SubQueryId(queryId, nextId.incrementAndGet());
  }
  
  public synchronized static ScheduleUnitId newScheduleUnitId(SubQueryId subQueryId) {
    return new ScheduleUnitId(subQueryId, nextId.incrementAndGet());
  }

  public synchronized static QueryUnitId newQueryUnitId(ScheduleUnitId scheduleUnitId) {
    return new QueryUnitId(scheduleUnitId, nextId.incrementAndGet());
  }

  public synchronized static QueryUnitAttemptId newQueryUnitAttemptId(
      final QueryUnitId queryUnitId, final int attemptId) {
    return new QueryUnitAttemptId(queryUnitId, attemptId);
  }
}
