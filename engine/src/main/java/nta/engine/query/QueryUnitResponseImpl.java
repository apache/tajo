/**
 * 
 */
package nta.engine.query;

import nta.engine.LeafServerProtos.QueryStatus;
import nta.engine.QueryUnitProtos.QueryUnitResponseProto;
import nta.engine.QueryUnitProtos.QueryUnitResponseProtoOrBuilder;
import nta.engine.ipc.protocolrecords.QueryUnitResponse;

/**
 * @author jihoon
 *
 */
public class QueryUnitResponseImpl implements QueryUnitResponse {
	
	private int id;
	private QueryStatus status;
	private String outputPath;
	
	private QueryUnitResponseProto proto = QueryUnitResponseProto.getDefaultInstance();
	private QueryUnitResponseProto.Builder builder = null;
	private boolean viaProto = false;
	
	public QueryUnitResponseImpl() {
		builder = QueryUnitResponseProto.newBuilder();
		this.id = -1;
	}
	
	public QueryUnitResponseImpl(int id, QueryStatus status, String outputPath) {
		this.id = id;
		this.status = status;
		this.outputPath = outputPath;
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

	/* (non-Javadoc)
	 * @see nta.engine.ipc.protocolrecords.QueryUnitResponse#getOutputPath()
	 */
	@Override
	public String getOutputPath() {
		QueryUnitResponseProtoOrBuilder p = viaProto ? proto : builder;
		if (this.outputPath != null) {
			return this.outputPath;
		}
		if (!proto.hasOutputPath()) {
			return null;
		}
		this.outputPath = p.getOutputPath();
		return this.outputPath;
	}

	@Override
	public int getId() {
		QueryUnitResponseProtoOrBuilder p = viaProto ? proto : builder;
		if (id != -1) {
			return this.id;
		}
		if (!proto.hasId()) {
			return -1;
		}
		this.id = p.getId();
		return this.id;
	}

	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = QueryUnitResponseProto.newBuilder(proto);
		}
		viaProto = true;
	}
	
	private void mergeLocalToBuilder() {
		if (id != -1) {
			builder.setId(this.id);
		}
		if (this.status != null) {
			builder.setStatus(this.status);
		}
		if (this.outputPath != null) {
			builder.setOutputPath(this.outputPath);
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
}
