package tajo.worker.dataserver.retriever;

import java.io.IOException;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public interface RetrieverHandler {
  /**
   *
   * @param kvs url-decoded key/value pairs
   * @return a desired part of a file
   * @throws IOException
   */
  public FileChunk get(Map<String, String> kvs) throws IOException;
}
