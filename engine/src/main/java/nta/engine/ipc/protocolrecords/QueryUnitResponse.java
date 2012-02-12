/**
 * 
 */
package nta.engine.ipc.protocolrecords;

import nta.common.ProtoObject;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.QueryUnitResponseProto;
import nta.engine.LeafServerProtos.QueryStatus;

/**
 * @author jihoon
 *
 */
public interface QueryUnitResponse extends ProtoObject<QueryUnitResponseProto> {

	public QueryUnitId getId();
	public QueryStatus getStatus();
}
