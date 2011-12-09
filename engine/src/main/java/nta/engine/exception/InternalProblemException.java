/**
 * 
 */
package nta.engine.exception;

/**
 * @author Hyunsik Choi
 *
 */
public class InternalProblemException extends NTAQueryException {
	private static final long serialVersionUID = -3674227022716578326L;
	
	public InternalProblemException() {};
	
	public InternalProblemException(Exception e) {
		super(e);
	};
}
