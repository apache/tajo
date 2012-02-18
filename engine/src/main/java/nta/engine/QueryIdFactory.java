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
  private static QueryUnitId queryUnitId;
  private static AtomicInteger nextQueryId= new AtomicInteger(-1);
  private static AtomicInteger nextSubQueryId = new AtomicInteger(-1);
  private static AtomicInteger nextQueryStepId = new AtomicInteger(-1);
  private static AtomicInteger nextQueryUnitId = new AtomicInteger(-1);
  
  public static void reset() {
    Date dateNow = new Date();
    SimpleDateFormat dateformatYYYYMMDD = new SimpleDateFormat("yyyyMMddSS");
    timeId = dateformatYYYYMMDD.format(dateNow);
    nextQueryId.set(-1); 
    nextSubQueryId.set(-1);
    nextQueryStepId.set(-1);
    nextQueryUnitId.set(-1);
  }

  public static QueryId newQueryId() {
    queryId = new QueryId(timeId, nextQueryId.incrementAndGet());
    nextSubQueryId.set(-1);
    return queryId;
  }
  
  public static SubQueryId newSubQueryId() {
    if (nextQueryId.equals(-1)) {
      newQueryId();
    }
    subQueryId = new SubQueryId(queryId, nextSubQueryId.incrementAndGet());
    nextQueryUnitId.set(-1);
    return subQueryId;
  }
  
  public static QueryUnitId newQueryUnitId() {
    if (nextQueryStepId.equals(-1)) {
      newQueryStepId();
    }
    queryUnitId = new QueryUnitId(queryStepId, nextQueryUnitId.incrementAndGet());
    return queryUnitId;
  }
  
  public static QueryStepId newQueryStepId() {
    if (nextSubQueryId.equals(-1)) {
      newSubQueryId();
    }
    queryStepId = new QueryStepId(subQueryId, nextQueryStepId.incrementAndGet());
    return queryStepId;
  }
  
  public static String nextQueryStepId() {
    if (nextSubQueryId.equals(-1)) {
      newSubQueryId();
    }
    return subQueryId.toString() + "_" + nextQueryStepId.incrementAndGet();
  }
}
