/**
 * 
 */
package nta.engine.ipc.protocolrecords;

import nta.common.ProtoObject;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.MasterInterfaceProtos.QueryUnitResponseProto;
import nta.engine.QueryUnitId;

/**
 * @author jihoon
 *
 */
public interface QueryUnitResponse extends ProtoObject<QueryUnitResponseProto> {

	public QueryUnitId getId();
	public QueryStatus getStatus();
}
