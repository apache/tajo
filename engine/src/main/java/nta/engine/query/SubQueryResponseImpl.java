/**
 * 
 */
package nta.engine.query;

import nta.engine.LeafServerProtos.QueryStatus;
import nta.engine.LeafServerProtos.SubQueryResponseProto;
import nta.engine.LeafServerProtos.SubQueryResponseProtoOrBuilder;
import nta.engine.ipc.protocolrecords.SubQueryResponse;

/**
 * @author jihoon
 *
 */
public class SubQueryResponseImpl implements SubQueryResponse {

  private int id;
	private QueryStatus status;
	
	private SubQueryResponseProto proto = SubQueryResponseProto.getDefaultInstance();
	private SubQueryResponseProto.Builder builder = null;
	private boolean viaProto = false;
	
	public SubQueryResponseImpl() {
		builder = SubQueryResponseProto.newBuilder();
		id = -1;
	}
	
	public SubQueryResponseImpl(int id, QueryStatus status) {
		this();
		set(id, status);
	}
	
	public SubQueryResponseImpl(SubQueryResponseProto proto) {
		this.proto = proto;
		viaProto = true;
	}
	
	public void set(int id, QueryStatus status) {
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
		if (id != -1) {
			builder.setId(id);
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
  public int getId() {
    SubQueryResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id != -1) {
      return this.id;
    }
    if (!p.hasId()) {
      return -1;
    }
    this.id = p.getId();
    return this.id;
  }
}
