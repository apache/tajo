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
	
	private QueryStatus status;
	private String outputPath;
	
	private SubQueryResponseProto proto = SubQueryResponseProto.getDefaultInstance();
	private SubQueryResponseProto.Builder builder = null;
	private boolean viaProto = false;
	
	public SubQueryResponseImpl() {
		builder = SubQueryResponseProto.newBuilder();
	}
	
	public SubQueryResponseImpl(QueryStatus status, String outputPath) {
		this();
		set(status, outputPath);
	}
	
	public SubQueryResponseImpl(SubQueryResponseProto proto) {
		this.proto = proto;
		viaProto = true;
	}
	
	public void set(QueryStatus status, String outputPath) {
		this.status = status;
		this.outputPath = outputPath;
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

	@Override
	public String getOutputPath() {
		SubQueryResponseProtoOrBuilder p = viaProto ? proto : builder;
		
		if (outputPath != null) {
			return this.outputPath;
		}
		if (!proto.hasOutputPath()) {
			return null;
		}
		this.outputPath = p.getOutputPath();
		return outputPath;
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
		if (outputPath != null) {
			builder.setOutputPath(outputPath);
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
}
