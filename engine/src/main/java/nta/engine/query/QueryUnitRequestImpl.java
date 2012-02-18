/**
 * 
 */
package nta.engine.query;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.Expose;

import nta.distexec.DistPlan;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.QueryUnitRequestProto;
import nta.engine.QueryUnitProtos.QueryUnitRequestProtoOrBuilder;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.json.GsonCreator;

/**
 * @author jihoon
 *
 */
public class QueryUnitRequestImpl implements QueryUnitRequest{
	
  @Expose
	private QueryUnitId id;
  @Expose
	private List<Fragment> fragments;
  @Expose
	private String outputTable;
	private boolean isUpdated;
	@Expose
	private boolean clusteredOutput;
	@Expose
	private String serializedData;     // logical node
	
	private QueryUnitRequestProto proto = QueryUnitRequestProto.getDefaultInstance();
	private QueryUnitRequestProto.Builder builder = null;
	private boolean viaProto = false;
	
	public QueryUnitRequestImpl() {
		builder = QueryUnitRequestProto.newBuilder();
		this.id = null;
		this.isUpdated = false;
	}
	
	public QueryUnitRequestImpl(QueryUnitId id, List<Fragment> fragments, 
			String outputTable, boolean clusteredOutput, 
			String serializedData) {
		this();
		this.set(id, fragments, outputTable, clusteredOutput, serializedData);
	}
	
	public QueryUnitRequestImpl(QueryUnitRequestProto proto) {
		this.proto = proto;
		viaProto = true;
		id = null;
		isUpdated = false;
	}
	
	public void set(QueryUnitId id, List<Fragment> fragments, 
			String outputTable, boolean clusteredOutput, 
			String serializedData) {
		this.id = id;
		this.fragments = fragments;
		this.outputTable = outputTable;
		this.clusteredOutput = clusteredOutput;
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
	public QueryUnitId getId() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (id != null) {
			return this.id;
		}
		if (!proto.hasId()) {
			return null;
		}
		this.id = new QueryUnitId(p.getId());
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
		if (id != null) {
			builder.setId(this.id.toString());
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

  @Override
  public void initFromProto() {
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (id == null && p.hasId()) {
      this.id = new QueryUnitId(p.getId());
    }
    if (fragments == null && p.getFragmentsCount() > 0) {
      this.fragments = new ArrayList<Fragment>();
      for (int i = 0; i < p.getFragmentsCount(); i++) {
        this.fragments.add(new Fragment(p.getFragments(i)));
      }
    }
    if (outputTable == null && p.hasOutputTable()) {
      this.outputTable = p.getOutputTable();
    }
    if (isUpdated == false && p.hasClusteredOutput()) {
      this.clusteredOutput = p.getClusteredOutput();
    }
    if (serializedData == null && p.hasSerializedData()) {
      this.serializedData = p.getSerializedData();
    }
  }
  
}
