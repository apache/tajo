/**
 * 
 */
package nta.engine.ipc;

import java.util.Collection;

import nta.common.ProtoObject;
import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.MasterInterfaceProtos.PingRequestProto;

/**
 * @author jihoon
 *
 */
public interface PingRequest extends ProtoObject<PingRequestProto> {
  Collection<InProgressStatusProto> getProgressList();
}
