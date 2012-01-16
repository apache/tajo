package nta.catalog;

import nta.engine.ipc.protocolrecords.Tablet;

public class TabletServInfo {

	private String hostName;
	private int port;
	private Tablet tablet;
	
	public TabletServInfo() {
		
	}
	
	public TabletServInfo(String hostName, int port, Tablet tablet) {
		this.set(hostName, port, tablet);
	}
	
	public void set(String hostName, int port, Tablet tablet) {
		this.hostName = hostName;
		this.port = port;
		this.tablet = tablet;
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
	
	public Tablet getTablet() {
		return this.tablet;
	}
	
	public String toString() {
		return new String("HostName: " + hostName + " port: " + port + " tablet: " + tablet);
	}
}
