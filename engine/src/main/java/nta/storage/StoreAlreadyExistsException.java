/**
 * 
 */
package nta.storage;

import java.io.IOException;

/**
 * @author hyunsik
 *
 */
public class StoreAlreadyExistsException extends IOException {
	private static final long serialVersionUID = -3998097009687418241L;

	/**
	 * 
	 */
	public StoreAlreadyExistsException() {
	}

	/**
	 * @param message
	 */
	public StoreAlreadyExistsException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public StoreAlreadyExistsException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public StoreAlreadyExistsException(String message, Throwable cause) {
		super(message, cause);
	}
}
