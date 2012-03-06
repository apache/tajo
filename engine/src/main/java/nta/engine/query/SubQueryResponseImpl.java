/**
 * 
 */
package nta.engine.query;

import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.MasterInterfaceProtos.SubQueryResponseProto;
import nta.engine.QueryUnitId;
import nta.engine.ipc.protocolrecords.SubQueryResponse;

/**
 * @author jihoon
 *
 */
public class SubQueryResponseImpl implements SubQueryResponse {

  private QueryUnitId id;
	private QueryStatus status;
	
	private SubQueryResponseProto proto = SubQueryResponseProto.getDefaultInstance();
	private SubQueryResponseProto.Builder builder = null;
	private boolean viaProto = false;
	
	public SubQueryResponseImpl() {
		builder = SubQueryResponseProto.newBuilder();
		id = null;
	}
	
	public SubQueryResponseImpl(QueryUnitId id, QueryStatus status) {
		this();
		set(id, status);
	}
	
	public SubQueryResponseImpl(SubQueryResponseProto proto) {
		this.proto = proto;
		viaProto = true;
	}
	
	public void set(QueryUnitId id, QueryStatus status) {
    this.id = id;
		this.status = status;
	}

	public SubQueryResponseProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}
	
	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = SubQueryResponseProto.newBuilder(proto);
		}
		viaProto = false;
	}
	
	private void mergeLocalToBuilder() {
	}
	
	private void mergeLocalToProto() {
		if (viaProto) {
			maybeInitBuilder();
		}
		mergeLocalToBuilder();
		proto = builder.build();
		viaProto = true;
	}

  @Override
  public void initFromProto() {
    // TODO Auto-generated method stub
    
  }
}
