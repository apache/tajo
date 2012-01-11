package nta.engine.plan.global;

import nta.engine.ipc.protocolrecords.SubQueryRequest;

/**
 * 
 * @author jihoon
 *
 */
public class DecomposedQuery implements Comparable<DecomposedQuery> {

	private String hostName;
	private SubQueryRequest query;
	
	public DecomposedQuery() {
		
	}
	
	public DecomposedQuery(SubQueryRequest query, String hostName) {
		this.set(query, hostName);
	}
	
	public void set(SubQueryRequest query, String hostName) {
		this.query = query;
		this.hostName = hostName;
	}
	
	public String getHostName() {
		return this.hostName;
	}
	
	public SubQueryRequest getQuery() {
		return this.query;
	}

	@Override
	public int compareTo(DecomposedQuery o) {
		return this.hostName.compareTo(o.getHostName());
	}
}
