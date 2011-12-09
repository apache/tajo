/**
 * 
 */
package nta.storage;

import java.io.IOException;

/**
 * @author hyunsik
 *
 */
public class StoreManagerException extends IOException {
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public StoreManagerException() {
	}

	/**
	 * @param message
	 */
	public StoreManagerException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public StoreManagerException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public StoreManagerException(String message, Throwable cause) {
		super(message, cause);
	}
}
