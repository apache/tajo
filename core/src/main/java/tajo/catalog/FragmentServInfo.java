package tajo.catalog;

import tajo.engine.cluster.ServerName;
import tajo.engine.ipc.protocolrecords.Fragment;


public class FragmentServInfo {
  private ServerName serverName;
	private Fragment fragment;
	
	public FragmentServInfo() {

	}
	
	public FragmentServInfo(String serverName, Fragment fragment) {
    this.set(serverName, fragment);
	}
	
	public FragmentServInfo(String hostName, int port, Fragment fragment) {
		this.set(hostName, port, fragment);
	}
	
	public void set(String hostName, int port, Fragment fragment) {
	  this.set(hostName+":"+port, fragment);
	}
	
	public void set(String serverName, Fragment fragment) {
	  this.serverName = new ServerName(serverName);    
	  this.fragment = fragment;
	}
	
	public void setHost(String host, int port) {
	  setHost(host+":"+port);    
	}
	
	public void setHost(String hostName) {
	  this.serverName = new ServerName(hostName);
	}
	
	public String getHostName() {
		return this.serverName.getHostname();
	}
	
	public int getPort() {
		return this.serverName.getPort();
	}
	
	public Fragment getFragment() {
		return this.fragment;
	}
	
	public String toString() {
		return new String(serverName + " " + fragment);
	}
	
	@Override
	public boolean equals(Object o) {
	  if (o instanceof FragmentServInfo) {
	    FragmentServInfo other = (FragmentServInfo) o;
	    if (this.serverName.equals(other.serverName) && 
	        this.fragment.equals(other.fragment)) {
	      return true;
	    }
	  }
	  return false;
	}
}
