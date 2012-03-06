/**
 * 
 */
package nta.engine.ipc.protocolrecords;

import java.util.List;

import nta.common.ProtoObject;
import nta.engine.MasterInterfaceProtos.QueryUnitRequestProto;
import nta.engine.QueryUnitId;

/**
 * @author jihoon
 *
 */
public interface QueryUnitRequest extends ProtoObject<QueryUnitRequestProto> {

	public QueryUnitId getId();
	public List<Fragment> getFragments();
	public String getOutputTableId();
	public boolean isClusteredOutput();
	public String getSerializedData();
}
