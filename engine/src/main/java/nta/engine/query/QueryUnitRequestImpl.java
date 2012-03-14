/**
 * 
 */
package nta.engine.query;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.ColumnProto;
import nta.catalog.proto.CatalogProtos.SchemaProtoOrBuilder;
import nta.engine.MasterInterfaceProtos.Fetch;
import nta.engine.MasterInterfaceProtos.QueryUnitRequestProto;
import nta.engine.MasterInterfaceProtos.QueryUnitRequestProtoOrBuilder;
import nta.engine.QueryUnitId;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;

import com.google.common.collect.Lists;
import com.google.gson.annotations.Expose;

/**
 * @author jihoon
 *
 */
public class QueryUnitRequestImpl implements QueryUnitRequest {
	
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
	@Expose
	private Boolean interQuery;
	@Expose
	private List<Fetch> fetches;
	
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
		if (!p.hasId()) {
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
		if (!p.hasOutputTable()) {
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
		if (!p.hasClusteredOutput()) {
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
		if (!p.hasSerializedData()) {
			return null;
		}
		this.serializedData = p.getSerializedData();
		return this.serializedData;
	}
	
	public boolean isInterQuery() {
	  QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (interQuery != null) {
      return interQuery;
    }
    if (!p.hasInterQuery()) {
      return false;
    }
    this.interQuery = p.getInterQuery();
    return this.interQuery;
	}
	
	public void setInterQuery() {
	  maybeInitBuilder();
	  this.interQuery = true;
	}
	
	public void addFetch(String name, URI uri) {
	  maybeInitBuilder();
	  initFetches();
	  fetches.add(
	  Fetch.newBuilder()
	    .setName(name)
	    .setUrls(uri.toString()).build());
	  
	}
	
	public List<Fetch> getFetches() {
	  initFetches();    
    return this.fetches;  
	}
	
	private void initFetches() {
	  if (this.fetches != null) {
      return;
    }
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.fetches = new ArrayList<Fetch>();
    for(Fetch fetch : p.getFetchesList()) {
      fetches.add(fetch);
    }
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
		if (this.interQuery != null) {
		  builder.setInterQuery(this.interQuery);
		}
		if (this.fetches != null) {
		  builder.addAllFetches(this.fetches);
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
    if (interQuery == null && p.hasInterQuery()) {
      this.interQuery = p.getInterQuery();
    }
    if (fetches == null && p.getFetchesCount() > 0) {
      this.fetches = p.getFetchesList();
    }
  }
  
}
