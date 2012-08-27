/**
 * 
 */
package tajo.engine.ipc;

import tajo.common.ProtoObject;
import tajo.engine.MasterInterfaceProtos.InProgressStatusProto;
import tajo.engine.MasterInterfaceProtos.PingRequestProto;

import java.util.Collection;

/**
 * @author jihoon
 *
 */
public interface PingRequest extends ProtoObject<PingRequestProto> {
  Collection<InProgressStatusProto> getProgressList();
}
