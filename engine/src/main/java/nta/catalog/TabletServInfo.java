package nta.catalog;

import nta.engine.ipc.protocolrecords.Tablet;

public class TabletServInfo {

	private String hostName;
	private Tablet tablet;
	
	public TabletServInfo() {
		
	}
	
	public TabletServInfo(String hostName, Tablet tablet) {
		this.set(hostName, tablet);
	}
	
	public void set(String hostName, Tablet tablet) {
		this.hostName = hostName;
		this.tablet = tablet;
	}
	
	public String getHostName() {
		return this.hostName;
	}
	
	public Tablet getTablet() {
		return this.tablet;
	}
	
	public String toString() {
		return new String("HostName: " + hostName + " tablet: " + tablet);
	}
}
