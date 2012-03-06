/**
 * 
 */
package nta.engine.ipc;

import java.util.Collection;

import nta.common.ProtoObject;
import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.MasterInterfaceProtos.QueryUnitReportProto;

/**
 * @author jihoon
 *
 */
public interface QueryUnitReport extends ProtoObject<QueryUnitReportProto> {
  public Collection<InProgressStatus> getProgressList();
}
