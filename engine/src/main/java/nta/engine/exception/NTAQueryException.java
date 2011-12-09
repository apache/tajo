/**
 * 
 */
package nta.engine.exception;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 *
 */
public class NTAQueryException extends IOException {
	private static final long serialVersionUID = -5012296598261064705L;

	public NTAQueryException() {
	}
	
	public NTAQueryException(Exception e) {
		super(e);
	}

	/**
	 * @param query
	 */
	public NTAQueryException(String query) {
		super(query);
	}
}
