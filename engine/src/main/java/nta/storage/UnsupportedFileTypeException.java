/**
 * 
 */
package nta.storage;

/**
 * @author Hyunsik Choi
 *
 */
public class UnsupportedFileTypeException extends RuntimeException {
	private static final long serialVersionUID = -8160289695849000342L;

	public UnsupportedFileTypeException() {
	}

	/**
	 * @param message
	 */
	public UnsupportedFileTypeException(String message) {
		super(message);
	}
}
