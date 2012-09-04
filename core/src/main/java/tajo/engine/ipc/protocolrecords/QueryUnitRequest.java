/**
 * 
 */
package tajo.engine.ipc.protocolrecords;

import tajo.QueryUnitAttemptId;
import tajo.common.ProtoObject;
import tajo.engine.MasterInterfaceProtos.Fetch;
import tajo.engine.MasterInterfaceProtos.QueryUnitRequestProto;

import java.net.URI;
import java.util.List;

/**
 * @author jihoon
 *
 */
public interface QueryUnitRequest extends ProtoObject<QueryUnitRequestProto> {

	public QueryUnitAttemptId getId();
	public List<Fragment> getFragments();
	public String getOutputTableId();
	public boolean isClusteredOutput();
	public String getSerializedData();
	public boolean isInterQuery();
	public void setInterQuery();
	public void addFetch(String name, URI uri);
	public List<Fetch> getFetches();
}
