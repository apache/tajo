/**
 * 
 */
package nta.engine.ipc.protocolrecords;

import java.util.List;

import nta.common.ProtoObject;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.QueryUnitRequestProto;

import com.google.gson.Gson;

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
