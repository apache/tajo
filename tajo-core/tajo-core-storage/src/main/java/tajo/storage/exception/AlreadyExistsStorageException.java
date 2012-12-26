/**
 * 
 */
package tajo.storage.exception;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author hyunsik
 *
 */
public class AlreadyExistsStorageException extends IOException {
  private static final long serialVersionUID = 965518916144019032L;


  public AlreadyExistsStorageException(String path) {
    super("Error: "+path+" alreay exists");    
  }
  
  public AlreadyExistsStorageException(Path path) {
    this(path.toString());
  }
}
