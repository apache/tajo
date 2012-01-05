/**
 * 
 */
package nta.engine.query;

import nta.engine.ipc.protocolrecords.QueryStatus;
import nta.engine.ipc.protocolrecords.SubQueryResponse;

/**
 * @author jihoon
 *
 */
public class SubQueryResponseImpl implements SubQueryResponse {
	
	private QueryStatus status;
	
	public SubQueryResponseImpl() {
		
	}
	
	public SubQueryResponseImpl(QueryStatus status) {
		set(status);
	}
	
	public void set(QueryStatus status) {
		this.status = status;
	}

	/* (non-Javadoc)
	 * @see nta.engine.ipc.protocolrecords.SubQueryResponse#getStatus()
	 */
	@Override
	public QueryStatus getStatus() {
		return this.status;
	}

}
