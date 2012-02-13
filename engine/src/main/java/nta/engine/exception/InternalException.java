/**
 * 
 */
package nta.engine.exception;

import java.io.IOException;

/**
 * @author hyunsik
 *
 */
public class InternalException extends IOException {

	private static final long serialVersionUID = -262149616685882358L;

	/**
	 * 
	 */
	public InternalException() {
	}
	
	public InternalException(String message) {
		super(message);
	}
	
	public InternalException(String message, Exception t) {
	  super(message, t);
	}
	
	public InternalException(Exception t) {
	  super(t);
	}
}
