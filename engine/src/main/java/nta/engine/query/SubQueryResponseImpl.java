/**
 * 
 */
package nta.engine.query;

import nta.engine.QueryUnitId;
import nta.engine.LeafServerProtos.QueryStatus;
import nta.engine.LeafServerProtos.SubQueryResponseProto;
import nta.engine.LeafServerProtos.SubQueryResponseProtoOrBuilder;
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

	/* (non-Javadoc)
	 * @see nta.engine.ipc.protocolrecords.SubQueryResponse#getStatus()
	 */
	@Override
	public QueryStatus getStatus() {
		SubQueryResponseProtoOrBuilder p = viaProto ? proto : builder;
		
		if (status != null) {
			return this.status;
		}
		if (!proto.hasStatus()) {
			return null;
		}
		this.status = p.getStatus();
		return this.status;
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
		if (status != null) {
			builder.setStatus(status);
		}
		if (id != null) {
			builder.setId(id.toString());
		}
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

  @Override
  public QueryUnitId getId() {
    SubQueryResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id != null) {
      return this.id;
    }
    if (!p.hasId()) {
      return null;
    }
    this.id = new QueryUnitId(p.getId());
    return this.id;
  }
}
