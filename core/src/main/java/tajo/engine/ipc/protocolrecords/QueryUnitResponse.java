/**
 * 
 */
package tajo.engine.ipc.protocolrecords;

import tajo.QueryUnitId;
import tajo.common.ProtoObject;
import tajo.engine.MasterInterfaceProtos.QueryStatus;
import tajo.engine.MasterInterfaceProtos.QueryUnitResponseProto;

/**
 * @author jihoon
 *
 */
public interface QueryUnitResponse extends ProtoObject<QueryUnitResponseProto> {

	public QueryUnitId getId();
	public QueryStatus getStatus();
}
