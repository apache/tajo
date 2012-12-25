package tajo.worker.dataserver.retriever;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * @author Hyunsik Choi
 */
public class FileChunk {
  private final File file;
  private final long startOffset;
  private final long length;

  public FileChunk(File file, long startOffset, long length) throws FileNotFoundException {
    this.file = file;
    this.startOffset = startOffset;
    this.length = length;
  }

  public File getFile() {
    return this.file;
  }

  public long startOffset() {
    return this.startOffset;
  }

  public long length() {
    return this.length;
  }

  public String toString() {
    return " (start=" + startOffset() + ", length=" + length + ") "
        + file.getAbsolutePath();
  }
}
