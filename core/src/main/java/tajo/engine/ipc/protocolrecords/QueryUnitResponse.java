/**
 * 
 */
package tajo.engine.ipc.protocolrecords;

import tajo.common.ProtoObject;
import tajo.engine.MasterInterfaceProtos.QueryStatus;
import tajo.engine.MasterInterfaceProtos.QueryUnitResponseProto;
import tajo.QueryUnitId;

/**
 * @author jihoon
 *
 */
public interface QueryUnitResponse extends ProtoObject<QueryUnitResponseProto> {

	public QueryUnitId getId();
	public QueryStatus getStatus();
}
