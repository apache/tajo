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
 * @author hyunsik
 * @author jihoon
 * 
 */
public class SubQueryRequestImpl implements SubQueryRequest {
	private int id;
  private List<Fragment> tablets;
  private URI dest;
  private String query;
  private String tableName;
  
  private SubQueryRequestProto proto = SubQueryRequestProto.getDefaultInstance();
  private SubQueryRequestProto.Builder builder = null;
  private boolean viaProto = false;
  
  public SubQueryRequestImpl() {
	  builder = SubQueryRequestProto.newBuilder();
	  this.id = -1;
  }
  
  public SubQueryRequestImpl(int id, List<Fragment> tablets, URI output, String query, String tableName) {
	  this();
	  this.id = id;
    this.tablets = tablets;
    this.dest = output;
    this.query = query;
    this.tableName = tableName;
  }

  public SubQueryRequestImpl(SubQueryRequestProto proto) {
	  this.proto = proto;
	  viaProto = true;
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
	  if (tablets != null) {
		  return this.tablets;
	  }
	  if (tablets == null) {
		  tablets = new ArrayList<Fragment>();
	  }
	  for (int i = 0; i < p.getTabletsCount(); i++) {
		  tablets.add(new Fragment(p.getTablets(i)));
	  }
	  return this.tablets;
  }

  @Override
  public URI getOutputDest() {
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
  
  public String getTableName() {
	  SubQueryRequestProtoOrBuilder p = viaProto ? proto : builder;
	  if (tableName != null) {
		  return this.tableName;
	  }
	  if (!proto.hasTableName()) {
		  return null;
	  }
	  this.tableName = p.getTableName();
	  return this.tableName;
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
	  if (tablets != null) {
		  for (int i = 0; i < tablets.size(); i++) {
			  builder.addTablets(tablets.get(i).getProto());
		  }
	  }
	  
	  if (this.dest != null) {
		  builder.setDest(this.dest.toString());
	  }
	  
	  if (this.query != null) {
		  builder.setQuery(this.query);
	  }
	  
	  if (this.tableName != null) {
		  builder.setTableName(this.tableName);
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
}
