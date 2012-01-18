package nta.engine.ipc.protocolrecords;

import java.net.URI;
import java.util.List;

import nta.common.ProtoObject;
import nta.engine.LeafServerProtos.SubQueryRequestProto;

/**
 * This contains the rewrote query and its global query plan. 
 * 
 * @author hyunsik
 */
public interface SubQueryRequest extends ProtoObject<SubQueryRequestProto> {
	public String getQuery();
	
	public String getTableName();

  public List<Fragment> getFragments();

  public URI getOutputDest();
}
