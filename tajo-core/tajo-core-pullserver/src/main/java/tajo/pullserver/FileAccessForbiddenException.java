package tajo.pullserver;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public class FileAccessForbiddenException extends IOException {
  private static final long serialVersionUID = -3383272565826389213L;

  public FileAccessForbiddenException() {
  }

  public FileAccessForbiddenException(String message) {
    super(message);
  }

  public FileAccessForbiddenException(Throwable cause) {
    super(cause);
  }

  public FileAccessForbiddenException(String message, Throwable cause) {
    super(message, cause);
  }
}
