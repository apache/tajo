package nta.engine.ipc.protocolrecords;

import java.net.URI;
import java.util.List;

import nta.common.ProtoObject;
import nta.engine.LeafServerProtos.SubQueryRequestProto;

/**
 * This contains the rewrote query and a part of a global query plan. 
 * 
 * @author Hyunsik Choi
 */
public interface SubQueryRequest extends ProtoObject<SubQueryRequestProto> {
	public int getId();
	
	public String getQuery();

  public List<Fragment> getFragments();

  public URI getOutputPath();
}
