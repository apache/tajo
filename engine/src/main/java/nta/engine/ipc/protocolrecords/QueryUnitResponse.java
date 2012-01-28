/**
 * 
 */
package nta.engine.ipc.protocolrecords;

import nta.common.ProtoObject;
import nta.engine.QueryUnitProtos.QueryUnitResponseProto;
import nta.engine.LeafServerProtos.QueryStatus;

/**
 * @author jihoon
 *
 */
public interface QueryUnitResponse extends ProtoObject<QueryUnitResponseProto> {

	public int getId();
	public QueryStatus getStatus();
	public String getOutputPath();
}
