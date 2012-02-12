/**
 * 
 */
package nta.engine;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author jihoon
 *
 */
public class QueryIdFactory {
  private static String timeId;
  private static QueryId queryId;
  private static SubQueryId subQueryId;
  private static QueryUnitId queryUnitId;
  private static int nextQueryId = -1;
  private static int nextSubQueryId = -1;
  private static int nextQueryUnitId = -1;
  
  public static void reset() {
    Date dateNow = new Date();
    SimpleDateFormat dateformatYYYYMMDD = new SimpleDateFormat("yyyyMMddSS");
    timeId = dateformatYYYYMMDD.format(dateNow);
    nextQueryId = -1;
    nextSubQueryId = -1;
    nextQueryUnitId = -1;
  }

  public static QueryId newQueryId() {
    queryId = new QueryId(timeId, ++nextQueryId);
    nextSubQueryId = -1;
    return queryId;
  }
  
  public static SubQueryId newSubQueryId() {
    if (nextQueryId == -1) {
      newQueryId();
    }
    subQueryId = new SubQueryId(queryId, ++nextSubQueryId);
    nextQueryUnitId = -1;
    return subQueryId;
  }
  
  public static QueryUnitId newQueryUnitId() {
    if (nextSubQueryId == -1) {
      newSubQueryId();
    }
    queryUnitId = new QueryUnitId(subQueryId, ++nextQueryUnitId);
    return queryUnitId;
  }
}
