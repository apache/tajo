/**
 * 
 */
package nta.catalog;

import java.net.URI;

/**
 * @author Hyunsik Choi
 *
 */
public class AlreadyRegisteredURIException extends RuntimeException {

	private static final long serialVersionUID = 747390434221048348L;

	public AlreadyRegisteredURIException() {
	}

	/**
	 * @param uri
	 */
	public AlreadyRegisteredURIException(String uri) {
		super("Already registered TRID: "+uri);
	}
	
	public AlreadyRegisteredURIException(URI uri) {
		this("Already registered TRID: "+uri);
	}
}
