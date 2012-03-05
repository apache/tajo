/**
 * 
 */
package nta.engine;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jihoon
 *
 */
public class QueryIdFactory {
  private static String timeId;
  private static QueryId queryId;
  private static SubQueryId subQueryId;
  private static QueryStepId queryStepId;
  private static LogicalQueryUnitId logicalQueryUnitId;
  private static QueryUnitId queryUnitId;
  private static AtomicInteger nextQueryId = 
      new AtomicInteger(-1);
  private static AtomicInteger nextSubQueryId = 
      new AtomicInteger(-1);
  private static AtomicInteger nextQueryStepId = 
      new AtomicInteger(-1);
  private static AtomicInteger nextLogicalQueryUnitId = 
      new AtomicInteger(-1);
  private static AtomicInteger nextQueryUnitId = 
      new AtomicInteger(-1);
  
  public static void reset() {
    Date dateNow = new Date();
    SimpleDateFormat dateformatYYYYMMDD = new SimpleDateFormat("yyyyMMddSS");
    timeId = dateformatYYYYMMDD.format(dateNow);
    nextQueryId.set(-1); 
    nextSubQueryId.set(-1);
    nextQueryStepId.set(-1);
    nextLogicalQueryUnitId.set(-1);
    nextQueryUnitId.set(-1);
  }

  public synchronized static QueryId newQueryId() {
    queryId = new QueryId(timeId, nextQueryId.incrementAndGet());
    nextSubQueryId.set(-1);
    return queryId;
  }
  
  public synchronized static SubQueryId newSubQueryId() {
    if (nextQueryId.get() == -1) {
      newQueryId();
    }
    subQueryId = new SubQueryId(queryId, nextSubQueryId.incrementAndGet());
    nextLogicalQueryUnitId.set(-1);
    return subQueryId;
  }
  
  public synchronized static LogicalQueryUnitId newLogicalQueryUnitId() {
    if (nextSubQueryId.get() == -1) {
      newSubQueryId();
    }
    logicalQueryUnitId = new LogicalQueryUnitId(subQueryId, 
        nextLogicalQueryUnitId.incrementAndGet());
    nextQueryUnitId.set(-1);
    return logicalQueryUnitId;
  }

  public synchronized static QueryUnitId newQueryUnitId() {
    if (nextQueryStepId.get() ==-1) {
      newQueryStepId();
    }
    queryUnitId = new QueryUnitId(logicalQueryUnitId, 
        nextQueryUnitId.incrementAndGet());
    return queryUnitId;
  }
  
  public synchronized static QueryStepId newQueryStepId() {
    if (nextSubQueryId.get() == -1) {
      newSubQueryId();
    }
    queryStepId = new QueryStepId(subQueryId, nextQueryStepId.incrementAndGet());
    return queryStepId;
  }
  
}
