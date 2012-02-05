package nta.engine.cluster;

import nta.common.ProtoObject;
import nta.engine.cluster.ClusterProtos.ServerNameProto;
import nta.engine.cluster.ClusterProtos.ServerNameProtoOrBuilder;

import com.google.common.base.Preconditions;

/**
 * @author hyunsik
 *
 */
public class ServerName2 implements ProtoObject<ServerNameProto> {
	private ServerNameProto proto;
	private ServerNameProto.Builder builder;
	private boolean viaProto = false;
	
	private String serverName;
	private String hostName;
	private int port = -1;
	
	/**
	 * 
	 */
	public ServerName2() {
		this.builder = ServerNameProto.newBuilder();
	}
	
	public ServerName2(String hostName, int port) {
		this();
		Preconditions.checkNotNull(hostName);
		Preconditions.checkArgument(port > 0);		
		
		this.serverName = hostName+":"+port;
		this.hostName = hostName;
		this.port = port;
	}
	
	public ServerName2(ServerNameProto proto) {
		Preconditions.checkNotNull(proto);		
		this.proto = proto;
		this.viaProto = true;
	}
	
	public String getServerName() {
		ServerNameProtoOrBuilder p = viaProto ? proto : builder;
		
		if(this.serverName != null) {
			return this.serverName;
		}
		this.serverName = p.getServerName();
		
		return this.serverName;
	}
	
	public void setServerName(String serverName) {
		maybeInitBuilder();
		this.serverName = serverName;
	}
	
	public String getHostName() {
		ServerNameProtoOrBuilder p = viaProto ? proto : builder;
		
		if(this.hostName != null) {
			return this.hostName;
		}
		this.serverName = p.getServerName();
		
		return this.serverName;
	}
	
	public void setHostName(String hostName) {
		maybeInitBuilder();
		this.hostName = hostName;
	}
	
	public int getPort() {
		ServerNameProtoOrBuilder p = viaProto ? proto : builder;
		if(this.port > 0) {
			return this.port;
		}
		this.port = p.getPort();
		
		return this.port;
	}

	@Override
	public ServerNameProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		
		return proto;
	}
	
	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = ServerNameProto.newBuilder(proto);
		}
		viaProto = false;
	}
	
	private void mergeLocalToBuilder() {
		if(this.serverName != null) {
			builder.setServerName(this.serverName);
		}
		if(this.hostName != null) {
			builder.setHostName(this.hostName);
		}
		if(this.port > 0) {
			builder.setPort(this.port);
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
	public boolean equals(Object o) {
		if (o instanceof ServerName2) {
			return this.getServerName().equals(((ServerName2)o).getServerName());
		}
		return false;
	}
	
	public int hashCode() {
		return getServerName().hashCode();
	}

  @Override
  public void initFromProto() {
    // TODO Auto-generated method stub
    
  }
}
