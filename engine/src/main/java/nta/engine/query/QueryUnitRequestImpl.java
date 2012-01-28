/**
 * 
 */
package nta.engine.query;

import java.util.ArrayList;
import java.util.List;

import nta.engine.QueryUnitProtos.QueryUnitRequestProto;
import nta.engine.QueryUnitProtos.QueryUnitRequestProtoOrBuilder;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;

/**
 * @author jihoon
 *
 */
public class QueryUnitRequestImpl implements QueryUnitRequest{
	
	private int id;
	private List<Fragment> fragments;
	private String outputTable;
	private boolean isUpdated;
	private boolean clusteredOutput;
	private String serializedClassName;
	private String serializedData;
	
	private QueryUnitRequestProto proto = QueryUnitRequestProto.getDefaultInstance();
	private QueryUnitRequestProto.Builder builder = null;
	private boolean viaProto = false;
	
	public QueryUnitRequestImpl() {
		builder = QueryUnitRequestProto.newBuilder();
		this.id = -1;
		this.isUpdated = false;
	}
	
	public QueryUnitRequestImpl(int id, List<Fragment> fragments, 
			String outputTable, boolean clusteredOutput, 
			String serializedClassName, String serializedData) {
		this();
		this.set(id, fragments, outputTable, clusteredOutput, serializedClassName, serializedData);
	}
	
	public QueryUnitRequestImpl(QueryUnitRequestProto proto) {
		this.proto = proto;
		viaProto = true;
	}
	
	public void set(int id, List<Fragment> fragments, 
			String outputTable, boolean clusteredOutput, 
			String serializedClassName, String serializedData) {
		this.id = id;
		this.fragments = fragments;
		this.outputTable = outputTable;
		this.clusteredOutput = clusteredOutput;
		this.serializedClassName = serializedClassName;
		this.serializedData = serializedData;
		this.isUpdated = true;
	}

	@Override
	public QueryUnitRequestProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}

	@Override
	public int getId() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (id != -1) {
			return this.id;
		}
		if (!proto.hasId()) {
			return -1;
		}
		this.id = p.getId();
		return this.id;
	}

	@Override
	public List<Fragment> getFragments() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (fragments != null) {
			return fragments;
		}
		if (fragments == null) {
			fragments = new ArrayList<Fragment>();
		}
		for (int i = 0; i < p.getFragmentsCount(); i++) {
			fragments.add(new Fragment(p.getFragments(i)));
		}
		return this.fragments;
	}

	@Override
	public String getOutputTableId() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (outputTable != null) {
			return this.outputTable;
		}
		if (!proto.hasOutputTable()) {
			return null;
		}
		this.outputTable = p.getOutputTable();
		return this.outputTable;
	}

	@Override
	public boolean isClusteredOutput() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (isUpdated) {
			return this.clusteredOutput;
		}
		if (!proto.hasClusteredOutput()) {
			return false;
		}
		this.clusteredOutput = p.getClusteredOutput();
		this.isUpdated = true;
		return this.clusteredOutput;
	}

	@Override
	public String getSerializedClassName() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (this.serializedClassName != null) {
			return this.serializedClassName;
		}
		if (!proto.hasSerializedClassName()) {
			return null;
		}
		this.serializedClassName = p.getSerializedClassName();
		return this.serializedClassName;
	}

	@Override
	public String getSerializedData() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (this.serializedData != null) {
			return this.serializedData;
		}
		if (!proto.hasSerializedData()) {
			return null;
		}
		this.serializedData = p.getSerializedData();
		return this.serializedData;
	}

	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = QueryUnitRequestProto.newBuilder(proto);
		}
		viaProto = true;
	}
	
	private void mergeLocalToBuilder() {
		if (id != -1) {
			builder.setId(this.id);
		}
		if (fragments != null) {
			for (int i = 0; i < fragments.size(); i++) {
				builder.addFragments(fragments.get(i).getProto());
			}
		}
		if (this.outputTable != null) {
			builder.setOutputTable(this.outputTable);
		}
		if (this.isUpdated) {
			builder.setClusteredOutput(this.clusteredOutput);
		}
		if (this.serializedClassName != null) {
			builder.setSerializedClassName(this.serializedClassName);
		}
		if (this.serializedData != null) {
			builder.setSerializedData(this.serializedData);
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
