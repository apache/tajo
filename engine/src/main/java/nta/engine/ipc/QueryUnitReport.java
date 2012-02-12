/**
 * 
 */
package nta.engine.ipc;

import nta.common.ProtoObject;
import nta.engine.QueryUnitProtos.QueryUnitReportProto;
import nta.engine.QueryUnitId;

/**
 * @author jihoon
 *
 */
public interface QueryUnitReport extends ProtoObject<QueryUnitReportProto> {

  public QueryUnitId getId();
  public float getProgress();
}
