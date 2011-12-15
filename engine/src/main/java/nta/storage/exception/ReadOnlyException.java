package nta.storage.exception;

import java.io.IOException;

public class ReadOnlyException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5003966361252921819L;

	public ReadOnlyException() {
		
	}
	
	public ReadOnlyException(Exception e) {
		super(e);
	}
}
