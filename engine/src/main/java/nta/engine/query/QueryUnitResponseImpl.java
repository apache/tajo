/**
 * 
 */
package nta.engine.query;

import nta.engine.LeafServerProtos.QueryStatus;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.QueryUnitResponseProto;
import nta.engine.QueryUnitProtos.QueryUnitResponseProtoOrBuilder;
import nta.engine.ipc.protocolrecords.QueryUnitResponse;

/**
 * @author jihoon
 *
 */
public class QueryUnitResponseImpl implements QueryUnitResponse {
	
	private QueryUnitId id;
	private QueryStatus status;
	
	private QueryUnitResponseProto proto = QueryUnitResponseProto.getDefaultInstance();
	private QueryUnitResponseProto.Builder builder = null;
	private boolean viaProto = false;
	
	public QueryUnitResponseImpl() {
		builder = QueryUnitResponseProto.newBuilder();
		this.id = null;
	}
	
	public QueryUnitResponseImpl(QueryUnitId id, QueryStatus status) {
		this.id = id;
		this.status = status;
	}
	
	public QueryUnitResponseImpl(QueryUnitResponseProto proto) {
		this.proto = proto;
		viaProto = true;
	}

	/* (non-Javadoc)
	 * @see nta.common.ProtoObject#getProto()
	 */
	@Override
	public QueryUnitResponseProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}

	/* (non-Javadoc)
	 * @see nta.engine.ipc.protocolrecords.QueryUnitResponse#getStatus()
	 */
	@Override
	public QueryStatus getStatus() {
		QueryUnitResponseProtoOrBuilder p = viaProto ? proto : builder;
		if (this.status != null) {
			return this.status;
		}
		if (!proto.hasStatus()) {
			return null;
		}
		this.status = p.getStatus();
		return this.status;
	}

	@Override
	public QueryUnitId getId() {
		QueryUnitResponseProtoOrBuilder p = viaProto ? proto : builder;
		if (id != null) {
			return this.id;
		}
		if (!proto.hasId()) {
			return null;
		}
		this.id = new QueryUnitId(p.getId());
		return this.id;
	}

	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = QueryUnitResponseProto.newBuilder(proto);
		}
		viaProto = true;
	}
	
	private void mergeLocalToBuilder() {
		if (id != null) {
			builder.setId(this.id.toString());
		}
		if (this.status != null) {
			builder.setStatus(this.status);
		}
	}
	
	private void mergeLocalToProto() {
		if(viaProto) {
			maybeInitBuilder();
		}
		mergeLocalToBuilder();
		proto = builder.build();
		viaProto = true;
	}

  @Override
  public void initFromProto() {
    QueryUnitResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id == null && p.hasId()) {
      this.id = new QueryUnitId(p.getId());
    }
    if (this.status == null && p.hasStatus()) {
      this.status = p.getStatus();
    }
  }
}
