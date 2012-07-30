package nta.storage;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public interface SeekableScanner extends Scanner {

  public abstract long getNextOffset() throws IOException;

  public abstract void seek(long offset) throws IOException;
}
