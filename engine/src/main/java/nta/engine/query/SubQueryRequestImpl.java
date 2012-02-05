package nta.engine.query;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import nta.engine.LeafServerProtos.SubQueryRequestProto;
import nta.engine.LeafServerProtos.SubQueryRequestProtoOrBuilder;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.ipc.protocolrecords.Fragment;

/**
 * @author Hyunsik Choi
 * @author jihoon
 * 
 */
public class SubQueryRequestImpl implements SubQueryRequest {
  private List<Fragment> fragments;
	private int id;
  private URI dest;
  private String query;
  
  private SubQueryRequestProto proto = SubQueryRequestProto.getDefaultInstance();
  private SubQueryRequestProto.Builder builder = null;
  private boolean viaProto = false;
  
  public SubQueryRequestImpl() {
	  builder = SubQueryRequestProto.newBuilder();
	  this.id = -1;
  }
  
  public SubQueryRequestImpl(int id, List<Fragment> fragments, URI output, String query) {
	  this();
	  this.id = id;
    this.fragments = fragments;
    this.dest = output;
    this.query = query;
  }

  public SubQueryRequestImpl(SubQueryRequestProto proto) {
	  this.proto = proto;
	  viaProto = true;
	  id = -1;
  }

  @Override
  public String getQuery() {
	  SubQueryRequestProtoOrBuilder p = viaProto ? proto : builder;
	  
	  if (query != null) {
		  return this.query;
	  }
	  if (!proto.hasQuery()) {
		  return null;
	  }
	  this.query = p.getQuery();
	  return this.query;
  }

  @Override
  public List<Fragment> getFragments() {
	  SubQueryRequestProtoOrBuilder p = viaProto ? proto : builder;
	  if (fragments != null) {
		  return this.fragments;
	  }
	  if (fragments == null) {
		  fragments = new ArrayList<Fragment>();
	  }
	  for (int i = 0; i < p.getTabletsCount(); i++) {
		  fragments.add(new Fragment(p.getTablets(i)));
	  }
	  return this.fragments;
  }

  @Override
  public URI getOutputPath() {
	  SubQueryRequestProtoOrBuilder p = viaProto ? proto : builder;
	  if (dest != null) {
		  return this.dest;
	  }
	  if (!proto.hasDest()) {
		  return null;
	  }
	  try {
		this.dest = new URI(p.getDest());
	} catch (URISyntaxException e) {
		e.printStackTrace();
	}
	  return this.dest;
  }
  
  public SubQueryRequestProto getProto() {
	  mergeLocalToProto();
	  proto = viaProto ? proto : builder.build();
	  viaProto = true;
	  return proto;
  }
  
  private void maybeInitBuilder() {
	  if (viaProto || builder == null) {
		  builder = SubQueryRequestProto.newBuilder(proto);
	  }
	  viaProto = false;
  }
  
  protected void mergeLocalToBuilder() {
    if (id != -1) {
      builder.setId(this.id);
    }
    if (fragments != null) {
      for (int i = 0; i < fragments.size(); i++) {
        builder.addTablets(fragments.get(i).getProto());
      }
    }

    if (this.dest != null) {
      builder.setDest(this.dest.toString());
    }

    if (this.query != null) {
      builder.setQuery(this.query);
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
  public int getId() {
	  SubQueryRequestProtoOrBuilder p = viaProto ? proto : builder;
	  if (id != -1) {
		  return this.id;
	  }
	  if (proto.hasId()) {
		  return -1;
	  }
	  this.id = p.getId();
	  return this.id;
  }

  @Override
  public void initFromProto() {
    // TODO Auto-generated method stub
    
  }
}
