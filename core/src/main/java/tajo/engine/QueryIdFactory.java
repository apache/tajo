/**
 * 
 */
package tajo.engine;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jihoon
 *
 */
public class QueryIdFactory {
  private static String timeId;
  private static AtomicInteger nextId = 
      new AtomicInteger(-1);
  
  public static void reset() {
    Date dateNow = new Date();
    SimpleDateFormat dateformatYYYYMMDD = new SimpleDateFormat("yyyyMMddSS");
    timeId = dateformatYYYYMMDD.format(dateNow);
    nextId.set(-1);
  }

  public synchronized static QueryId newQueryId() {
    return new QueryId(timeId, nextId.incrementAndGet());
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
