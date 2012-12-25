package tajo.pullserver.retriever;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public interface RetrieverHandler {
  /**
   *
   * @param kvs url-decoded key/value pairs
   * @return a desired part of a file
   * @throws java.io.IOException
   */
  public FileChunk get(Map<String, List<String>> kvs) throws IOException;
}
