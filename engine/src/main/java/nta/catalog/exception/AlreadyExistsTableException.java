package nta.catalog.exception;


/**
 * 
 * @author Hyunsik Choi
 *
 */
public class AlreadyExistsTableException extends CatalogException {
	private static final long serialVersionUID = -641623770742392865L;

	public AlreadyExistsTableException() {		
	}

	public AlreadyExistsTableException(String tableName) {
		super("Already Exists Table: "+tableName);
	}
}
