package nta.engine.ipc.protocolrecords;

import nta.common.ProtoObject;
import nta.engine.LeafServerProtos.SubQueryResponseProto;

/**
 * 
 * @author jihoon
 *
 */

public interface SubQueryResponse extends ProtoObject<SubQueryResponseProto> {

	public nta.engine.LeafServerProtos.QueryStatus getStatus();
	public String getOutputPath();
}
