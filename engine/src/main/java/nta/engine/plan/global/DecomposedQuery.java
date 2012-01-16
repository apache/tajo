package nta.engine.plan.global;

import nta.engine.ipc.protocolrecords.SubQueryRequest;

/**
 * 
 * @author jihoon
 *
 */
public class DecomposedQuery implements Comparable<DecomposedQuery> {

	private String hostName;
	private int port;
	private SubQueryRequest query;
	
	public DecomposedQuery() {
		
	}
	
	public DecomposedQuery(SubQueryRequest query, int port, String hostName) {
		this.set(query, port, hostName);
	}
	
	public void set(SubQueryRequest query, int port, String hostName) {
		this.query = query;
		this.port = port;
		this.hostName = hostName;
	}
	
	public String getHostName() {
		return this.hostName;
	}
	
	public int getPort() {
		return this.port;
	}
	
	public SubQueryRequest getQuery() {
		return this.query;
	}

	@Override
	public int compareTo(DecomposedQuery o) {
		return this.hostName.compareTo(o.getHostName());
	}
}
