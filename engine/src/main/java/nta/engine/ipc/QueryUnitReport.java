/**
 * 
 */
package nta.engine.ipc;

import java.util.Collection;

import nta.common.ProtoObject;
import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.QueryUnitProtos.QueryUnitReportProto;

/**
 * @author jihoon
 *
 */
public interface QueryUnitReport extends ProtoObject<QueryUnitReportProto> {
  public Collection<InProgressStatus> getProgressList();
}
