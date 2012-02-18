package nta.catalog;

import nta.engine.ipc.protocolrecords.Fragment;


public class HostInfo {

	private String hostName;
	private int port;
	private Fragment fragment;
	
	public HostInfo() {
		
	}
	
	public HostInfo(String hostName, int port, Fragment fragment) {
		this.set(hostName, port, fragment);
	}
	
	public void set(String hostName, int port, Fragment fragment) {
		this.hostName = hostName;
		this.port = port;
		this.fragment = fragment;
	}
	
	public void setHost(String host, int port) {
		this.hostName = host;
		this.port = port;
	}
	
	public String getHostName() {
		return this.hostName;
	}
	
	public int getPort() {
		return this.port;
	}
	
	public Fragment getFragment() {
		return this.fragment;
	}
	
	public String toString() {
		return new String(hostName + ":" + port + " " + fragment);
	}
}
